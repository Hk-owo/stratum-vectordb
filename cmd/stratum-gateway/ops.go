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
	"net/url"
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

// handler is what the gateway mounts at /ops/: the console's routes wrapped in
// the request guard that stands between a browser and a process manager.
//
// Two things make the guard necessary rather than nice to have (H2 of
// docs/参考/code-review-2026-09-24.md): the console can start, stop and reconfigure
// processes, and the default listener is reachable from a browser. A page on any
// other origin can POST here without the console's consent — no preflight is
// involved for a form or a no-cors fetch — and `POST /ops/stop` with an empty
// body stops everything.
func (m *opsManager) handler() http.Handler {
	return guardOpsRequests(m.opsMux)
}

// guardOpsRequests refuses state-changing requests that a browser made from
// another site.
//
// It is a CSRF check, not authentication: the console is expected to sit behind
// a network boundary or a reverse proxy with its own access control (the default
// listener is loopback-only for the same reason), and this closes the gap that a
// boundary does NOT close — a browser on the trusted network, holding the
// console's cookies or simply an open tab, being told to send the request by a
// page from somewhere else.
//
// Both signals are checked because neither is universal: Origin is sent for every
// cross-origin fetch but not by every client, and Sec-Fetch-Site is sent by modern
// browsers only. Absent signals are treated as allowed — a curl from the operator's
// shell has neither, and it is not the threat this guards against.
func guardOpsRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
			writeOpsError(w, http.StatusForbidden,
				"cross-site request refused: /ops changes process state and is not a CSRF endpoint")
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && !sameOriginAsRequest(origin, r.Host) {
			writeOpsError(w, http.StatusForbidden,
				"origin "+origin+" does not match this console ("+r.Host+"); /ops is same-origin only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameOriginAsRequest reports whether an Origin header names the very host the
// request was addressed to. A relative/opaque origin ("null") never matches.
func sameOriginAsRequest(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, host)
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
		// 脚本路径不再缓存在这里：它按拓扑从配置解析（见 dockerCluster.scriptPath），
		// 缓存一份就会在切换拓扑后继续驱动旧脚本。
		docker: &dockerCluster{},
		opsMux: http.NewServeMux(),
		client: &http.Client{Timeout: 3 * time.Second},
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

	// --- docker 集群管理（集群级统一参数，转调 scripts/cluster.sh） ---
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

func writeOpsJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOpsError(w http.ResponseWriter, code int, msg string) {
	writeOpsErrorCode(w, code, opsErrCode(code), msg)
}

func writeOpsErrorCode(w http.ResponseWriter, code int, errCode, msg string) {
	writeOpsJSON(w, code, map[string]string{
		"error": msg,
		"code":  errCode,
	})
}

func opsErrCode(httpCode int) string {
	switch httpCode {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHORIZED"
	case http.StatusForbidden:
		return "FORBIDDEN"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusBadGateway:
		return "BAD_GATEWAY"
	default:
		return "INTERNAL"
	}
}
