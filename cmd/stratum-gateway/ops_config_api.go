package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (m *opsManager) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	cfg := m.cfg
	m.mu.Unlock()
	writeOpsJSON(w, http.StatusOK, cfg)
}

// handlePutConfig accepts a full or partial OpsConfig JSON body, overlays
// it on the current config, and persists it. Running services keep their
// current parameters; edits apply on the next start/restart.
func (m *opsManager) handlePutConfig(w http.ResponseWriter, r *http.Request) {
	var patch OpsConfig
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeOpsError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// The patch is applied to a COPY and only published once it has passed every
	// check: a refused edit must leave the live config untouched, or the console
	// keeps running with the value it just told the caller it rejected.
	next := *m.cfg
	// Cluster is the only reference field in OpsConfig, so the copy above would share
	// its array with the object being replaced. Copy it too, and then publish a NEW
	// pointer instead of overwriting this one (M15 of docs/参考/code-review-2026-10-18.md):
	// the supervisor and every handler that takes `cfg := m.cfg` under m.mu and then
	// works outside it read a snapshot, so a snapshot must stop changing the moment it
	// is handed out. Writing through the old pointer raced those readers under a lock
	// nobody shared.
	next.Cluster = append([]ClusterNode(nil), m.cfg.Cluster...)
	// Edits that decide WHAT GETS EXECUTED are refused over HTTP, rather than
	// silently ignored (H2 of docs/参考/code-review-2026-09-24.md): bin_dir, the
	// cluster table forwardNode sends requests to, and the orchestration script
	// are the difference between "tune a parameter" and "run this program". They
	// belong to the deployment scripts, which write them next to the config.
	// Refusing loudly matters: an operator who sends one and sees 200 would
	// believe it took effect.
	if err := refuseExecSemanticEdits(m.cfg, patch); err != nil {
		writeOpsError(w, http.StatusForbidden, err.Error())
		return
	}
	if patch.NodeID != 0 {
		next.NodeID = patch.NodeID
	}
	if patch.LogDir != "" {
		next.LogDir = patch.LogDir
	}
	if patch.ConfigDir != "" {
		next.ConfigDir = patch.ConfigDir
	}
	// docker 段是集群级统一配置：patch 中一旦出现 docker 字段即整体替换。
	// （script / script_two_tier 已被上面的检查挡掉，其余字段只是参数。）
	if patch.Docker.Enabled || patch.Docker.Nodes != 0 ||
		patch.Docker.BasePort != 0 || patch.Docker.Network != "" || patch.Docker.Image != "" ||
		patch.Docker.ContainerPrefix != "" || patch.Docker.WithEmbed ||
		patch.Docker.Topology != "" {
		next.Docker = patch.Docker
	}
	mergeServices(&next, patch)
	applyOpsDefaults(&next)
	// 白名单校验放在合并之后、生效之前：这些值决定执行哪个文件，检查的对象必须是
	// 真正会被 supervisor / dockerCluster 用到的那个配置。
	if err := validateExecConfig(&next); err != nil {
		writeOpsError(w, http.StatusForbidden, err.Error())
		return
	}
	if err := saveOpsConfig(m.cfgPath, &next); err != nil {
		writeOpsError(w, http.StatusInternalServerError, "save config: "+err.Error())
		return
	}
	m.cfg = &next
	m.sup.SetConfig(m.cfg)
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"note": "参数已保存；正在运行的服务将在下次启动/重启时生效",
	})
}

// refuseExecSemanticEdits rejects an HTTP-supplied change to a field that
// selects code to run.
//
// These are not "parameters with a dangerous range" — they are the assignment
// itself: bin_dir + services.*.bin pick the executable, docker.script picks the
// program an operator's request will run, cluster picks the hosts a forwarded
// /ops request is sent to (SSRF), and station_secret is the key the node trusts.
// A console that can rewrite them over HTTP is a remote execution endpoint
// (H2 of docs/参考/code-review-2026-09-24.md), which is why the fix is to refuse the
// edit rather than to validate it: there is no safe new value for
// "run this other binary".
func refuseExecSemanticEdits(cur *OpsConfig, patch OpsConfig) error {
	if patch.BinDir != "" && patch.BinDir != cur.BinDir {
		return fmt.Errorf("bin_dir 由部署脚本写入，/ops 不允许通过 HTTP 修改（它决定可以执行哪个目录下的程序）")
	}
	if patch.Cluster != nil && !sameCluster(cur.Cluster, patch.Cluster) {
		return fmt.Errorf("cluster 由部署脚本写入，/ops 不允许通过 HTTP 修改（forwardNode 会按它向目标主机发请求）")
	}
	if v := patch.Docker.Script; v != "" && v != cur.Docker.Script {
		return fmt.Errorf("docker.script 由部署脚本写入，/ops 不允许通过 HTTP 修改（它是会被执行的编排脚本）")
	}
	if v := patch.Docker.ScriptTwoTier; v != "" && v != cur.Docker.ScriptTwoTier {
		return fmt.Errorf("docker.script_two_tier 由部署脚本写入，/ops 不允许通过 HTTP 修改（它是会被执行的编排脚本）")
	}
	// station_secret 与节点信任标记共用一份密钥：能从 HTTP 改它，就能让节点接受
	// 别人伪造的标记（H4）。
	if v := patch.Services.Stratum.StationSecret; v != "" && v != cur.Services.Stratum.StationSecret {
		return fmt.Errorf("station_secret 由部署脚本写入，/ops 不允许通过 HTTP 修改（它是节点校验服务站标记的密钥）")
	}
	return nil
}

// sameCluster compares the cluster table by value, so a patch that sends back
// exactly what it read is accepted.
func sameCluster(a, b []ClusterNode) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// validateExecConfig checks the executable-selection fields that remain
// editable, by whitelist: a service binary must be a bare name inside bin_dir (or
// an absolute path inside it), and an orchestration script must be a .sh file
// under <cwd>/scripts.
//
// This is the second half of H2's fix. Refusing the "which directory" edits
// above still leaves "which name inside it", and a name is enough: `bin_dir` plus
// `../../tmp/evil` is the same remote execution as rewriting bin_dir itself.
func validateExecConfig(cfg *OpsConfig) error {
	binDir := absOrSelf(cfg.BinDir)
	names := map[string]string{
		string(ServiceVecstore): cfg.Services.Vecstore.Bin,
		string(ServiceEmbed):    cfg.Services.Embed.Bin,
		string(ServiceStratum):  cfg.Services.Stratum.Bin,
	}
	for _, svc := range AllServices {
		name := names[string(svc)]
		if name == "" {
			continue
		}
		candidate := name
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(binDir, name)
		}
		if !isWithin(binDir, absOrSelf(candidate)) {
			return fmt.Errorf("%s 的可执行文件必须位于 bin_dir（%s）之内：%q", svc, binDir, name)
		}
	}
	for _, script := range []string{cfg.Docker.Script, cfg.Docker.ScriptTwoTier} {
		if script == "" {
			continue
		}
		if err := validateScriptPath(script); err != nil {
			return err
		}
	}
	return nil
}

// validateScriptPath confines docker orchestration scripts to <cwd>/scripts.
//
// The console runs from the repository root (scripts/gateway.sh cds there, and
// every relative path in the config — run/bin, run/configs — assumes it), so
// "under <cwd>/scripts and ending in .sh" is a check the deployment can actually
// satisfy while an attacker cannot: dockerCluster executes this path.
func validateScriptPath(script string) error {
	scriptsDir := absOrSelf(filepath.Join("scripts"))
	abs := absOrSelf(script)
	if !isWithin(scriptsDir, abs) || !strings.HasSuffix(abs, ".sh") {
		return fmt.Errorf("编排脚本必须位于 %s 下的 .sh 文件：%q", scriptsDir, script)
	}
	return nil
}

// absOrSelf resolves p to an absolute, cleaned path, falling back to the input
// when the working directory cannot be determined.
func absOrSelf(p string) string {
	if p == "" {
		return ""
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	return abs
}

// isWithin reports whether child is dir itself or sits underneath it.
func isWithin(dir, child string) bool {
	if dir == "" || child == "" {
		return false
	}
	if child == dir {
		return true
	}
	return strings.HasPrefix(child, strings.TrimSuffix(dir, string(os.PathSeparator))+string(os.PathSeparator))
}

// mergeServices overlays non-zero service fields from the patch.
func mergeServices(dst *OpsConfig, patch OpsConfig) {
	if patch.Services.Vecstore.Bin != "" {
		dst.Services.Vecstore.Bin = patch.Services.Vecstore.Bin
	}
	if patch.Services.Vecstore.GRPCAddr != "" {
		dst.Services.Vecstore.GRPCAddr = patch.Services.Vecstore.GRPCAddr
	}
	if patch.Services.Vecstore.RocksDBPath != "" {
		dst.Services.Vecstore.RocksDBPath = patch.Services.Vecstore.RocksDBPath
	}
	if patch.Services.Vecstore.HealthAddr != "" {
		dst.Services.Vecstore.HealthAddr = patch.Services.Vecstore.HealthAddr
	}
	if patch.Services.Vecstore.ExtraArgsRaw != "" {
		dst.Services.Vecstore.ExtraArgsRaw = patch.Services.Vecstore.ExtraArgsRaw
	}

	if patch.Services.Embed.Bin != "" {
		dst.Services.Embed.Bin = patch.Services.Embed.Bin
	}
	if patch.Services.Embed.ServiceAddr != "" {
		dst.Services.Embed.ServiceAddr = patch.Services.Embed.ServiceAddr
	}

	s, ps := &dst.Services.Stratum, &patch.Services.Stratum
	if ps.Bin != "" {
		s.Bin = ps.Bin
	}
	if ps.NodeID != 0 {
		s.NodeID = ps.NodeID
	}
	if ps.DataDir != "" {
		s.DataDir = ps.DataDir
	}
	if ps.GRPCAddr != "" {
		s.GRPCAddr = ps.GRPCAddr
	}
	if ps.RaftAddr != "" {
		s.RaftAddr = ps.RaftAddr
	}
	if len(ps.Peers) > 0 {
		s.Peers = ps.Peers
	}
	if ps.VecstoreAddr != "" {
		s.VecstoreAddr = ps.VecstoreAddr
	}
	if ps.EmbedAddr != "" {
		s.EmbedAddr = ps.EmbedAddr
	}
	if ps.HeartbeatIntervalMS != 0 {
		s.HeartbeatIntervalMS = ps.HeartbeatIntervalMS
	}
	if ps.ElectionTimeoutMinMS != 0 {
		s.ElectionTimeoutMinMS = ps.ElectionTimeoutMinMS
	}
	if ps.ElectionTimeoutMaxMS != 0 {
		s.ElectionTimeoutMaxMS = ps.ElectionTimeoutMaxMS
	}
	if ps.IndexLRUCapacity != 0 {
		s.IndexLRUCapacity = ps.IndexLRUCapacity
	}
	if ps.IndexLoadWaitTimeoutMS != 0 {
		s.IndexLoadWaitTimeoutMS = ps.IndexLoadWaitTimeoutMS
	}
	if ps.IndexCallbackMaxRetries != 0 {
		s.IndexCallbackMaxRetries = ps.IndexCallbackMaxRetries
	}
	if ps.IndexCallbackRetryBaseMS != 0 {
		s.IndexCallbackRetryBaseMS = ps.IndexCallbackRetryBaseMS
	}
	if ps.WriteMaxRetries != 0 {
		s.WriteMaxRetries = ps.WriteMaxRetries
	}
	if ps.WriteRetryBaseMS != 0 {
		s.WriteRetryBaseMS = ps.WriteRetryBaseMS
	}
	if ps.DeleteMaxRetries != 0 {
		s.DeleteMaxRetries = ps.DeleteMaxRetries
	}
	if ps.DeleteRetryBaseMS != 0 {
		s.DeleteRetryBaseMS = ps.DeleteRetryBaseMS
	}
}
