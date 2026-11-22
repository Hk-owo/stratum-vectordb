package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// --- docker 集群管理 handlers -------------------------------------------

// dockerCfg 返回当前 docker 集群统一配置；未启用时给出错误。
func (m *opsManager) dockerCfg(w http.ResponseWriter) (DockerClusterConfig, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.Docker.Enabled {
		writeOpsErrorCode(w, http.StatusBadRequest, "DOCKER_DISABLED",
			"docker 集群管理未启用：请在 ops config 中设置 docker.enabled=true")
		return DockerClusterConfig{}, false
	}
	return m.cfg.Docker, true
}

// handleDockerStatus 返回集群 JSON 状态（脚本 status N --json 原样输出）。
func (m *opsManager) handleDockerStatus(w http.ResponseWriter, _ *http.Request) {
	cfg, ok := m.dockerCfg(w)
	if !ok {
		return
	}
	out, err := m.docker.Status(cfg)
	if err != nil {
		writeOpsError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// handleDockerGetConfig 返回 docker 集群统一配置（无论 enabled 为 true 还是 false，
// 前端都可以正常读取当前配置，不必用 400 逼出盲打启用开关）。
func (m *opsManager) handleDockerGetConfig(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	cfg := m.cfg.Docker
	m.mu.Unlock()
	writeOpsJSON(w, http.StatusOK, cfg)
}

// handleDockerPutConfig 更新集群级统一参数并持久化（整个集群一起生效）。
// dockerClusterPatch is the body of PUT /ops/docker/config.
//
// Every field is a pointer so "absent" is distinguishable from "zero": with plain
// values, `{"network":"my-net"}` used to switch docker management OFF, because the
// omitted booleans decoded to false and were written through unconditionally. An
// operator would see the console's buttons stop working and have no reason to
// connect that to the parameter they just saved.
type dockerClusterPatch struct {
	Enabled         *bool   `json:"enabled"`
	Topology        *string `json:"topology"`
	Script          *string `json:"script"`
	ScriptTwoTier   *string `json:"script_two_tier"`
	Nodes           *int    `json:"nodes"`
	StorageNodes    *int    `json:"storage_nodes"`
	BasePort        *int    `json:"base_port"`
	StorageBasePort *int    `json:"storage_base_port"`
	Network         *string `json:"network"`
	Image           *string `json:"image"`
	ContainerPrefix *string `json:"container_prefix"`
	WithEmbed       *bool   `json:"with_embed"`
}

// handleDockerPutConfig 更新集群级统一参数并持久化（整个集群一起生效）。
//
// This is the SECOND way to write the config over HTTP, so it has to obey the same
// rules as handlePutConfig (C2 of docs/参考/code-review-2026-10-18.md). It used to obey
// none: `docker.script` was written straight through and `dockerCluster.run` execs
// that path, which meant one PUT plus POST /ops/docker/up was arbitrary command
// execution as the gateway — exactly what refusing the same field on /ops/config
// was meant to prevent, reached through the endpoint next to it.
func (m *opsManager) handleDockerPutConfig(w http.ResponseWriter, r *http.Request) {
	var patch dockerClusterPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeOpsError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Refused rather than validated, and refused loudly: there is no safe new value
	// for "run this other program", and an operator who sends one and sees 200 would
	// believe it took effect.
	if patch.Script != nil && *patch.Script != m.cfg.Docker.Script {
		writeOpsError(w, http.StatusForbidden,
			"docker.script 由部署脚本写入，/ops 不允许通过 HTTP 修改（它是会被执行的编排脚本）")
		return
	}
	if patch.ScriptTwoTier != nil && *patch.ScriptTwoTier != m.cfg.Docker.ScriptTwoTier {
		writeOpsError(w, http.StatusForbidden,
			"docker.script_two_tier 由部署脚本写入，/ops 不允许通过 HTTP 修改（它是会被执行的编排脚本）")
		return
	}

	// Applied to a COPY and published only after the write succeeds, so a refused or
	// failed save leaves the live config untouched.
	next := *m.cfg
	next.Cluster = append([]ClusterNode(nil), m.cfg.Cluster...)
	next.Docker = m.cfg.Docker
	d := &next.Docker
	if patch.Enabled != nil {
		d.Enabled = *patch.Enabled
	}
	if patch.Topology != nil {
		d.Topology = *patch.Topology
	}
	if patch.Nodes != nil {
		d.Nodes = *patch.Nodes
	}
	if patch.StorageNodes != nil {
		d.StorageNodes = *patch.StorageNodes
	}
	if patch.BasePort != nil {
		d.BasePort = *patch.BasePort
	}
	if patch.StorageBasePort != nil {
		d.StorageBasePort = *patch.StorageBasePort
	}
	if patch.Network != nil {
		d.Network = *patch.Network
	}
	if patch.Image != nil {
		d.Image = *patch.Image
	}
	if patch.ContainerPrefix != nil {
		d.ContainerPrefix = *patch.ContainerPrefix
	}
	if patch.WithEmbed != nil {
		d.WithEmbed = *patch.WithEmbed
	}

	// Deliberately NOT re-running validateExecConfig here. The scripts cannot have
	// changed through this handler (they are refused above), and the rest of the
	// fields only pick parameters, not programs. Re-validating the whole config would
	// turn any pre-existing oddity in a deployment's script path into a 403 on an
	// unrelated parameter edit — a check that fails on the wrong thing teaches
	// operators to work around it.
	if err := saveOpsConfig(m.cfgPath, &next); err != nil {
		writeOpsError(w, http.StatusInternalServerError, "save config: "+err.Error())
		return
	}
	m.cfg = &next
	m.sup.SetConfig(m.cfg)
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"note":   "集群参数已保存；对运行中的集群执行「启动/重建」后生效",
		"docker": m.cfg.Docker,
	})
}

// handleDockerLifecycle 实现 POST /ops/docker/up|down|clean。
// 参数变更后执行 up（--force）即按新参数重建整个集群。
func (m *opsManager) handleDockerLifecycle(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, ok := m.dockerCfg(w)
		if !ok {
			return
		}
		var out string
		var err error
		switch action {
		case "up":
			// body 可选 {"force":true}，强制按当前集群参数重建容器
			var body struct {
				Force bool `json:"force"`
			}
			if r.Body != nil {
				if data, _ := io.ReadAll(r.Body); len(strings.TrimSpace(string(data))) > 0 {
					_ = json.Unmarshal(data, &body)
				}
			}
			out, err = m.docker.Up(cfg, body.Force)
		case "down":
			out, err = m.docker.Down(cfg)
		case "clean":
			out, err = m.docker.Clean(cfg)
		}
		if err != nil {
			writeOpsError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOpsJSON(w, http.StatusOK, map[string]any{"ok": true, "output": out})
	}
}

// handleDockerNode 实现 POST /ops/docker/nodes/{id}/start|stop|restart。
func (m *opsManager) handleDockerNode(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, ok := m.dockerCfg(w)
		if !ok {
			return
		}
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || !dockerNodeIDValid(cfg, id) {
			writeOpsError(w, http.StatusBadRequest, dockerNodeIDHint(cfg))
			return
		}
		var out string
		switch action {
		case "start":
			out, err = m.docker.NodeStart(cfg, id)
		case "stop":
			out, err = m.docker.NodeStop(cfg, id)
		case "restart":
			out, err = m.docker.NodeRestart(cfg, id)
		}
		if err != nil {
			writeOpsError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOpsJSON(w, http.StatusOK, map[string]any{"ok": true, "output": out})
	}
}

// handleDockerNodeLogs 返回单个节点最近的日志文本。
func (m *opsManager) handleDockerNodeLogs(w http.ResponseWriter, r *http.Request) {
	cfg, ok := m.dockerCfg(w)
	if !ok {
		return
	}
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || !dockerNodeIDValid(cfg, id) {
		writeOpsError(w, http.StatusBadRequest, dockerNodeIDHint(cfg))
		return
	}
	lines := 200
	if v := r.URL.Query().Get("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 5000 {
			lines = n
		}
	}
	out, err := m.docker.NodeLogs(cfg, id, lines)
	if err != nil {
		writeOpsError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeOpsJSON(w, http.StatusOK, map[string]any{"id": id, "lines": lines, "log": out})
}

// tailFile returns up to n trailing lines of a file.
func tailFile(path string, n int) (lines []string, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	size := st.Size()
	if size == 0 {
		return []string{}, false, nil
	}
	// Read from the end in chunks until n newlines are found or EOF.
	chunk := int64(4096)
	buf := make([]byte, 0, chunk)
	pos := size
	for {
		start := pos - chunk
		if start < 0 {
			start = 0
		}
		seg := make([]byte, pos-start)
		if _, err := f.ReadAt(seg, start); err != nil {
			return nil, false, err
		}
		buf = append(seg, buf...)
		if bytesCount(buf, '\n') > n || start == 0 {
			break
		}
		pos = start
	}
	parts := splitLines(string(buf))
	if len(parts) > n {
		parts = parts[len(parts)-n:]
		truncated = true
	}
	return parts, truncated, nil
}

func bytesCount(b []byte, c byte) int {
	n := 0
	for _, x := range b {
		if x == c {
			n++
		}
	}
	return n
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	s = strings.TrimSuffix(s, "\n")
	return strings.Split(s, "\n")
}
