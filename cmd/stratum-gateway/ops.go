// /ops/* HTTP API: the console (control plane) endpoints served by every
// stratum-gateway. They work whether or not the local database stack is
// running, and let one console drive the whole cluster: the local node
// is handled in-process, remote nodes are reached by forwarding the same
// request to their gateway_addr.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// opsManager owns the console config + supervisor and serves /ops/*.
type opsManager struct {
	mu      sync.Mutex
	cfgPath string
	cfg     *OpsConfig
	sup     *Supervisor
	docker  *dockerCluster

	// opsMux holds all /ops/* routes (local + cross-node). The main mux
	// mounts it under /ops/. Local dispatch inside forwardNode re-enters
	// this mux with the prefix stripped.
	opsMux *http.ServeMux

	client *http.Client // for cross-node forwarding + liveness probes
}

// newOpsManager loads (or defaults) the console config. nodeID seeds the
// defaults when no config file exists yet.
func newOpsManager(cfgPath string, nodeID int) (*opsManager, error) {
	cfg, err := loadOpsConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	if cfg.NodeID == 0 {
		cfg.NodeID = nodeID
	}
	applyOpsDefaults(&cfg)
	// The local node must be routable via /ops/nodes/{id}, so ensure it is
	// present in the cluster. A leftover ID:0 default entry (written before
	// the node id was known) is replaced.
	hasLocal := false
	for i := range cfg.Cluster {
		if cfg.Cluster[i].ID == 0 {
			cfg.Cluster[i] = ClusterNode{ID: cfg.NodeID, GatewayAddr: "http://127.0.0.1:8081"}
			hasLocal = true
		}
		if cfg.Cluster[i].ID == cfg.NodeID {
			hasLocal = true
		}
	}
	if !hasLocal {
		cfg.Cluster = append([]ClusterNode{{ID: cfg.NodeID, GatewayAddr: "http://127.0.0.1:8081"}}, cfg.Cluster...)
	}
	m := &opsManager{
		cfgPath: cfgPath,
		cfg:     &cfg,
		sup:     NewSupervisor(&cfg),
		docker:  &dockerCluster{script: cfg.Docker.Script},
		opsMux:  http.NewServeMux(),
		client:  &http.Client{Timeout: 3 * time.Second},
	}
	m.registerRoutes(m.opsMux)
	return m, nil
}

func (m *opsManager) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /ops/health", m.handleHealth)
	mux.HandleFunc("GET /ops/nodes", m.handleNodes)
	mux.HandleFunc("GET /ops/status", m.handleStatus)
	mux.HandleFunc("POST /ops/start", m.handleStartStop("start"))
	mux.HandleFunc("POST /ops/stop", m.handleStartStop("stop"))
	mux.HandleFunc("POST /ops/restart", m.handleStartStop("restart"))
	mux.HandleFunc("GET /ops/config", m.handleGetConfig)
	mux.HandleFunc("PUT /ops/config", m.handlePutConfig)
	mux.HandleFunc("GET /ops/logs/{service}", m.handleLogs)

	// --- docker 集群管理（集群级统一参数，转调 docker-cluster.sh） ---
	mux.HandleFunc("GET /ops/docker/status", m.handleDockerStatus)
	mux.HandleFunc("GET /ops/docker/config", m.handleDockerGetConfig)
	mux.HandleFunc("PUT /ops/docker/config", m.handleDockerPutConfig)
	mux.HandleFunc("POST /ops/docker/up", m.handleDockerLifecycle("up"))
	mux.HandleFunc("POST /ops/docker/down", m.handleDockerLifecycle("down"))
	mux.HandleFunc("POST /ops/docker/clean", m.handleDockerLifecycle("clean"))
	mux.HandleFunc("POST /ops/docker/nodes/{id}/start", m.handleDockerNode("start"))
	mux.HandleFunc("POST /ops/docker/nodes/{id}/stop", m.handleDockerNode("stop"))
	mux.HandleFunc("POST /ops/docker/nodes/{id}/restart", m.handleDockerNode("restart"))
	mux.HandleFunc("GET /ops/docker/logs/{id}", m.handleDockerNodeLogs)

	// Cross-node: forward to the target node's gateway (same path minus
	// the /ops/nodes/{id} prefix).
	mux.HandleFunc("GET /ops/nodes/{id}/status", m.forwardNode)
	mux.HandleFunc("POST /ops/nodes/{id}/start", m.forwardNode)
	mux.HandleFunc("POST /ops/nodes/{id}/stop", m.forwardNode)
	mux.HandleFunc("POST /ops/nodes/{id}/restart", m.forwardNode)
	mux.HandleFunc("GET /ops/nodes/{id}/config", m.forwardNode)
	mux.HandleFunc("PUT /ops/nodes/{id}/config", m.forwardNode)
	mux.HandleFunc("GET /ops/nodes/{id}/logs/{service}", m.forwardNode)
}

// --- local handlers -------------------------------------------------

func (m *opsManager) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"node_id": m.nodeID(),
	})
}

// handleNodes returns the cluster list with liveness + (for the local
// node) service states.
func (m *opsManager) handleNodes(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	cfg := m.cfg
	sup := m.sup
	m.mu.Unlock()

	localID := m.nodeID()
	out := make([]map[string]any, 0, len(cfg.Cluster))
	for _, n := range cfg.Cluster {
		entry := map[string]any{
			"id":           n.ID,
			"gateway_addr": n.GatewayAddr,
			"online":       n.ID == localID, // filled below for remotes
			"local":        n.ID == localID,
		}
		if n.ID == localID {
			entry["services"] = sup.Status()
		} else if n.GatewayAddr != "" {
			entry["online"] = m.probe(n.GatewayAddr)
		}
		out = append(out, entry)
	}
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"local_node_id": localID,
		"nodes":         out,
	})
}

func (m *opsManager) handleStatus(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"node_id":  m.nodeID(),
		"services": sup.Status(),
	})
}

// handleStartStop implements POST /ops/start|stop|restart.
func (m *opsManager) handleStartStop(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, err := opsServicesFromBody(r)
		if err != nil {
			writeOpsError(w, http.StatusBadRequest, err.Error())
			return
		}
		m.mu.Lock()
		sup := m.sup
		m.mu.Unlock()

		results := make([]map[string]any, 0, len(services))
		for _, svc := range services {
			var opErr error
			switch action {
			case "start":
				opErr = sup.Start(svc)
			case "stop":
				opErr = sup.Stop(svc, 5*time.Second)
			case "restart":
				opErr = sup.Stop(svc, 5*time.Second)
				if opErr == nil {
					opErr = sup.Start(svc)
				}
			}
			entry := map[string]any{"service": svc}
			if opErr != nil {
				entry["error"] = opErr.Error()
			}
			results = append(results, entry)
		}
		writeOpsJSON(w, http.StatusOK, map[string]any{"ok": true, "results": results})
	}
}

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
	if patch.NodeID != 0 {
		m.cfg.NodeID = patch.NodeID
	}
	if patch.BinDir != "" {
		m.cfg.BinDir = patch.BinDir
	}
	if patch.LogDir != "" {
		m.cfg.LogDir = patch.LogDir
	}
	if patch.ConfigDir != "" {
		m.cfg.ConfigDir = patch.ConfigDir
	}
	if len(patch.Cluster) > 0 {
		m.cfg.Cluster = patch.Cluster
	}
	// docker 段是集群级统一配置：patch 中一旦出现 docker 字段即整体替换。
	if patch.Docker.Enabled || patch.Docker.Script != "" || patch.Docker.Nodes != 0 ||
		patch.Docker.BasePort != 0 || patch.Docker.Network != "" || patch.Docker.Image != "" ||
		patch.Docker.ContainerPrefix != "" || patch.Docker.WithEmbed {
		m.cfg.Docker = patch.Docker
	}
	mergeServices(m.cfg, patch)
	applyOpsDefaults(m.cfg)
	if err := saveOpsConfig(m.cfgPath, m.cfg); err != nil {
		writeOpsError(w, http.StatusInternalServerError, "save config: "+err.Error())
		return
	}
	m.sup.SetConfig(m.cfg)
	m.docker.script = m.cfg.Docker.Script
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"note": "参数已保存；正在运行的服务将在下次启动/重启时生效",
	})
}

// handleLogs tails the local service log file.
func (m *opsManager) handleLogs(w http.ResponseWriter, r *http.Request) {
	svc := ServiceID(r.PathValue("service"))
	if !validService(svc) {
		writeOpsError(w, http.StatusBadRequest, "unknown service "+string(svc))
		return
	}
	m.mu.Lock()
	logDir := m.cfg.LogDir
	m.mu.Unlock()

	lines := 200
	if v := r.URL.Query().Get("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 5000 {
			lines = n
		}
	}
	path := strings.TrimSuffix(logDir, "/") + "/" + string(svc) + ".log"
	tail, truncated, err := tailFile(path, lines)
	if err != nil && !os.IsNotExist(err) {
		writeOpsError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tail == nil {
		tail = []string{} // serialize as [] instead of null
	}
	writeOpsJSON(w, http.StatusOK, map[string]any{
		"service":   svc,
		"log_file":  path,
		"lines":     tail,
		"truncated": truncated,
	})
}

// --- cross-node forwarding ------------------------------------------

// forwardNode routes /ops/nodes/{id}/... to the target node's gateway.
func (m *opsManager) forwardNode(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeOpsError(w, http.StatusBadRequest, "invalid node id")
		return
	}

	m.mu.Lock()
	cfg := m.cfg
	m.mu.Unlock()

	if id == m.nodeID() {
		// Local: strip the /ops/nodes/{id} prefix and dispatch through
		// the ops mux (the plain local handlers).
		rest := "/ops" + strings.TrimPrefix(r.URL.Path, "/ops/nodes/"+r.PathValue("id"))
		r2 := r.Clone(r.Context())
		r2.URL.Path = rest
		m.opsMux.ServeHTTP(w, r2)
		return
	}

	var target string
	for _, n := range cfg.Cluster {
		if n.ID == id {
			target = n.GatewayAddr
			break
		}
	}
	if target == "" {
		writeOpsError(w, http.StatusNotFound, fmt.Sprintf("node %d not in cluster config", id))
		return
	}

	rest := "/ops" + strings.TrimPrefix(r.URL.Path, "/ops/nodes/"+r.PathValue("id"))
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target+rest, r.Body)
	if err != nil {
		writeOpsError(w, http.StatusInternalServerError, err.Error())
		return
	}
	req.Header = r.Header.Clone()
	req.URL.RawQuery = r.URL.RawQuery
	resp, err := m.client.Do(req)
	if err != nil {
		writeOpsError(w, http.StatusBadGateway, "node "+strconv.Itoa(id)+" unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// --- helpers ---------------------------------------------------------

func (m *opsManager) nodeID() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.NodeID
}

// probe checks whether a remote gateway is alive.
func (m *opsManager) probe(gatewayAddr string) bool {
	url := strings.TrimRight(gatewayAddr, "/") + "/ops/health"
	resp, err := m.client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// opsServicesFromBody parses {"services":[...]} (empty = all).
func opsServicesFromBody(r *http.Request) ([]ServiceID, error) {
	var body struct {
		Services []string `json:"services"`
	}
	if r.Body != nil {
		data, _ := io.ReadAll(r.Body)
		if len(strings.TrimSpace(string(data))) > 0 {
			if err := json.Unmarshal(data, &body); err != nil {
				return nil, fmt.Errorf("invalid JSON body: %v", err)
			}
		}
	}
	if len(body.Services) == 0 {
		return append([]ServiceID(nil), AllServices...), nil
	}
	out := make([]ServiceID, 0, len(body.Services))
	for _, s := range body.Services {
		svc := ServiceID(s)
		if !validService(svc) {
			return nil, fmt.Errorf("unknown service %q", s)
		}
		out = append(out, svc)
	}
	return out, nil
}

func validService(s ServiceID) bool {
	switch s {
	case ServiceVecstore, ServiceEmbed, ServiceStratum:
		return true
	}
	return false
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

func writeOpsJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOpsError(w http.ResponseWriter, code int, msg string) {
	writeOpsJSON(w, code, map[string]string{"error": msg})
}

// --- docker 集群管理 handlers -------------------------------------------

// dockerCfg 返回当前 docker 集群统一配置；未启用时给出错误。
func (m *opsManager) dockerCfg(w http.ResponseWriter) (DockerClusterConfig, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.Docker.Enabled {
		writeOpsError(w, http.StatusBadRequest,
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

// handleDockerGetConfig 返回 docker 集群统一配置。
func (m *opsManager) handleDockerGetConfig(w http.ResponseWriter, _ *http.Request) {
	cfg, ok := m.dockerCfg(w)
	if !ok {
		return
	}
	writeOpsJSON(w, http.StatusOK, cfg)
}

// handleDockerPutConfig 更新集群级统一参数并持久化（整个集群一起生效）。
func (m *opsManager) handleDockerPutConfig(w http.ResponseWriter, r *http.Request) {
	var patch DockerClusterConfig
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeOpsError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if patch.Script != "" {
		m.cfg.Docker.Script = patch.Script
	}
	if patch.Nodes > 0 {
		m.cfg.Docker.Nodes = patch.Nodes
	}
	if patch.BasePort > 0 {
		m.cfg.Docker.BasePort = patch.BasePort
	}
	if patch.Network != "" {
		m.cfg.Docker.Network = patch.Network
	}
	if patch.Image != "" {
		m.cfg.Docker.Image = patch.Image
	}
	if patch.ContainerPrefix != "" {
		m.cfg.Docker.ContainerPrefix = patch.ContainerPrefix
	}
	// 布尔字段允许显式关闭：WithEmbed/Enabled 以 patch 中的值为准。
	m.cfg.Docker.WithEmbed = patch.WithEmbed
	m.cfg.Docker.Enabled = patch.Enabled
	m.docker.script = m.cfg.Docker.Script
	if err := saveOpsConfig(m.cfgPath, m.cfg); err != nil {
		writeOpsError(w, http.StatusInternalServerError, "save config: "+err.Error())
		return
	}
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
		if err != nil || id < 1 || id > cfg.Nodes {
			writeOpsError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid node id（1-%d）", cfg.Nodes))
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
	if err != nil || id < 1 || id > cfg.Nodes {
		writeOpsError(w, http.StatusBadRequest,
			fmt.Sprintf("invalid node id（1-%d）", cfg.Nodes))
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
