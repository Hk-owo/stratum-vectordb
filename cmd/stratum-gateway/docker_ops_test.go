package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeDockerScript 生成一个假的 docker-cluster.sh：记录被调用的参数，
// 按子命令输出预置结果（status 输出 JSON，logs 输出文本，其余输出调用摘要）。
func fakeDockerScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fake-docker-cluster.sh")
	content := `#!/bin/sh
echo "called:$*" >> "$FAKE_CALL_LOG"
case " $* " in
  *" status "*)
    printf '%s' '{"network":"test-net","count":3,"base_port":17000,"image":"fake:latest","nodes":[{"id":1,"name":"node1","status":"running","health":"healthy","grpc_port":17000,"leader":false},{"id":2,"name":"node2","status":"running","health":"healthy","grpc_port":17001,"leader":true},{"id":3,"name":"node3","status":"absent","health":"-","grpc_port":17002,"leader":false}]}'
    ;;
  *" logs "*)
    echo "fake-node-log"
    ;;
  *)
    echo "action=$1"
    ;;
esac
`
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// testDockerOpsServer 构造启用 docker 集群管理的 ops 服务器。
// fake 脚本输出会被记录到 callLog 文件，便于断言 gateway 转调脚本的参数。
func testDockerOpsServer(t *testing.T) (*httptest.Server, *opsManager, string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "console.yaml")
	callLog := filepath.Join(dir, "calls.log")
	t.Setenv("FAKE_CALL_LOG", callLog)

	cfg := defaultOpsConfig(1)
	cfg.BinDir = dir
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.ConfigDir = filepath.Join(dir, "configs")
	cfg.Docker.Enabled = true
	cfg.Docker.Script = fakeDockerScript(t, dir)
	cfg.Docker.Nodes = 3
	if err := saveOpsConfig(cfgPath, &cfg); err != nil {
		t.Fatal(err)
	}

	m, err := newOpsManager(cfgPath, 1)
	if err != nil {
		t.Fatalf("newOpsManager: %v", err)
	}
	srv := httptest.NewServer(m.opsMux)
	t.Cleanup(srv.Close)
	return srv, m, callLog
}

// readCalls 读取 fake 脚本的调用日志（每次调用一行）并清空，便于后续断言。
func readCalls(t *testing.T, callLog string) string {
	t.Helper()
	data, err := os.ReadFile(callLog)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.WriteFile(callLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestOpsDockerStatus(t *testing.T) {
	srv, _, callLog := testDockerOpsServer(t)
	code, body := doOps(t, srv.URL+"/ops/docker/status", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("status: code=%d body=%v", code, body)
	}
	// 脚本 status 输出被原样透传
	if body["network"] != "test-net" {
		t.Errorf("network = %v, want test-net", body["network"])
	}
	nodes, ok := body["nodes"].([]any)
	if !ok || len(nodes) != 3 {
		t.Fatalf("nodes = %v, want 3 entries", body["nodes"])
	}
	n1 := nodes[0].(map[string]any)
	if n1["name"] != "node1" || n1["leader"] != false {
		t.Errorf("node1 = %v", n1)
	}
	// 脚本收到的参数应包含节点数与 --json
	logs := readCalls(t, callLog)
	if !strings.Contains(logs, "status 3 --json") {
		t.Errorf("expected 'status 3 --json' in script calls, got: %s", logs)
	}
}

func TestOpsDockerLifecycle(t *testing.T) {
	srv, _, callLog := testDockerOpsServer(t)
	for _, action := range []string{"up", "down", "clean"} {
		code, body := doOps(t, srv.URL+"/ops/docker/"+action, "POST", nil)
		if code != http.StatusOK {
			t.Fatalf("%s: code=%d body=%v", action, code, body)
		}
		if body["ok"] != true {
			t.Errorf("%s: ok=%v", action, body["ok"])
		}
	}
	// up 应带 with-embed（默认配置）与节点数
	logs := readCalls(t, callLog)
	if !strings.Contains(logs, "up 3") || !strings.Contains(logs, "--with-embed") {
		t.Errorf("expected 'up 3 --with-embed' in script calls, got: %s", logs)
	}
}

func TestOpsDockerUpForce(t *testing.T) {
	srv, _, callLog := testDockerOpsServer(t)
	code, _ := doOps(t, srv.URL+"/ops/docker/up", "POST", map[string]any{"force": true})
	if code != http.StatusOK {
		t.Fatalf("up force: code=%d", code)
	}
	logs := readCalls(t, callLog)
	if !strings.Contains(logs, "--force") {
		t.Errorf("expected --force in script calls, got: %s", logs)
	}
}

func TestOpsDockerNodeOps(t *testing.T) {
	srv, _, callLog := testDockerOpsServer(t)
	cases := []struct{ action, wantArg string }{
		{"start", "start 2"},
		{"stop", "stop 2"},
		{"restart", "restart 2"},
	}
	for _, c := range cases {
		code, body := doOps(t, srv.URL+"/ops/docker/nodes/2/"+c.action, "POST", nil)
		if code != http.StatusOK {
			t.Fatalf("%s: code=%d body=%v", c.action, code, body)
		}
		if body["ok"] != true {
			t.Errorf("%s: ok=%v", c.action, body["ok"])
		}
	}
	logs := readCalls(t, callLog)
	for _, want := range []string{"start 2", "stop 2", "restart 2"} {
		if !strings.Contains(logs, want) {
			t.Errorf("expected %q in script calls, got: %s", want, logs)
		}
	}
	// start/restart 前会先 init 确保配置存在
	if !strings.Contains(logs, "init 3") {
		t.Errorf("expected 'init 3' before node start, got: %s", logs)
	}
}

func TestOpsDockerInvalidNode(t *testing.T) {
	srv, _, _ := testDockerOpsServer(t)
	for _, id := range []string{"0", "9", "abc"} {
		code, body := doOps(t, srv.URL+"/ops/docker/nodes/"+id+"/stop", "POST", nil)
		if code != http.StatusBadRequest {
			t.Errorf("node %s: code=%d body=%v, want 400", id, code, body)
		}
	}
}

func TestOpsDockerLogs(t *testing.T) {
	srv, _, callLog := testDockerOpsServer(t)
	code, body := doOps(t, srv.URL+"/ops/docker/logs/2?lines=50", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("logs: code=%d body=%v", code, body)
	}
	if body["log"] != "fake-node-log" {
		t.Errorf("log = %v", body["log"])
	}
	// 脚本收到的参数含 --lines
	logs := readCalls(t, callLog)
	if !strings.Contains(logs, "logs 2 --lines 50") {
		t.Errorf("expected 'logs 2 --lines 50' in script calls, got: %s", logs)
	}
}

func TestOpsDockerConfigGetPut(t *testing.T) {
	srv, m, _ := testDockerOpsServer(t)

	code, body := doOps(t, srv.URL+"/ops/docker/config", "GET", nil)
	if code != http.StatusOK || body["nodes"].(float64) != 3 {
		t.Fatalf("get config: code=%d body=%v", code, body)
	}

	// 集群级统一参数修改（含显式关闭 with_embed）
	patch := map[string]any{
		"enabled":          true,
		"nodes":            5,
		"base_port":        20000,
		"network":          "my-net",
		"image":            "my-image:1",
		"container_prefix": "my-node",
		"with_embed":       false,
	}
	code, body = doOps(t, srv.URL+"/ops/docker/config", "PUT", patch)
	if code != http.StatusOK {
		t.Fatalf("put config: code=%d body=%v", code, body)
	}
	m.mu.Lock()
	cfg := *m.cfg
	m.mu.Unlock()
	if cfg.Docker.Nodes != 5 || cfg.Docker.BasePort != 20000 ||
		cfg.Docker.Network != "my-net" || cfg.Docker.Image != "my-image:1" ||
		cfg.Docker.ContainerPrefix != "my-node" || cfg.Docker.WithEmbed {
		t.Errorf("docker config not applied: %+v", cfg.Docker)
	}

	code, body = doOps(t, srv.URL+"/ops/docker/config", "GET", nil)
	if code != http.StatusOK || body["nodes"].(float64) != 5 {
		t.Errorf("get after put: code=%d body=%v", code, body)
	}
}

func TestOpsDockerDisabled(t *testing.T) {
	srv, m, _ := testDockerOpsServer(t)
	m.mu.Lock()
	m.cfg.Docker.Enabled = false
	m.mu.Unlock()
	code, body := doOps(t, srv.URL+"/ops/docker/status", "GET", nil)
	if code != http.StatusBadRequest {
		t.Errorf("disabled: code=%d body=%v, want 400", code, body)
	}
}
