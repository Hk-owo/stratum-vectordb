// Ops-config: the console (stratum-gateway) keeps its own YAML file
// (default ./run/console.yaml) holding the cluster node list plus the
// local service startup parameters. This lets the web console operate
// (inspect / start / stop / edit parameters) before the database itself
// is running.
//
// Path defaults are relative to the gateway's working directory so the
// one-click start.sh layout (run/bin, run/log, run/data) is reused as-is.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// OpsConfig is the on-disk console configuration for one node.
type OpsConfig struct {
	NodeID int `yaml:"node_id" json:"node_id"`

	// Local paths (defaults relative to the working directory).
	BinDir    string `yaml:"bin_dir" json:"bin_dir"`       // service executables (default ./run/bin)
	LogDir    string `yaml:"log_dir" json:"log_dir"`       // service logs (default ./run/log)
	ConfigDir string `yaml:"config_dir" json:"config_dir"` // generated stratum YAML configs (default ./run/configs)

	// Cluster: the nodes the console can drive. The local node must be
	// present; remote nodes are reached through their gateway_addr.
	Cluster []ClusterNode `yaml:"cluster" json:"cluster"`

	// Docker: 集群级 docker 管理参数（整个集群统一，不做单节点差异化修改）。
	// 启用后控制台通过转调 docker-cluster.sh 管理节点生命周期。
	Docker DockerClusterConfig `yaml:"docker" json:"docker"`

	Services ServiceConfigs `yaml:"services" json:"services"`
}

// DockerClusterConfig 描述控制台管理的 docker 集群。所有参数都是集群级
// 统一配置：修改后重建整个集群，而不是单独改某个节点。
type DockerClusterConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	Script          string `yaml:"script" json:"script"`                     // docker-cluster.sh 路径（相对工作目录）
	Nodes           int    `yaml:"nodes" json:"nodes"`                       // 集群节点数
	BasePort        int    `yaml:"base_port" json:"base_port"`               // 节点 1 的 gRPC 宿主端口（raft=+1000, metrics=+2000）
	Network         string `yaml:"network" json:"network"`                   // Docker 网络名
	Image           string `yaml:"image" json:"image"`                       // stratum 镜像名
	ContainerPrefix string `yaml:"container_prefix" json:"container_prefix"` // 容器名前缀（stratum-nodeN）
	WithEmbed       bool   `yaml:"with_embed" json:"with_embed"`             // 是否同时启动 mock-embed 依赖
}

// ClusterNode identifies one console/gateway endpoint in the cluster.
type ClusterNode struct {
	ID          int    `yaml:"id" json:"id"`
	GatewayAddr string `yaml:"gateway_addr" json:"gateway_addr"`
}

// ServiceConfigs holds the startup parameters of the three managed local
// services (vecstore / embed / stratum).
type ServiceConfigs struct {
	Vecstore VecstoreConfig `yaml:"vecstore" json:"vecstore"`
	Embed    EmbedConfig    `yaml:"embed" json:"embed"`
	Stratum  StratumConfig  `yaml:"stratum" json:"stratum"`
}

// VecstoreConfig is the C++ vecstore_server process parameters.
type VecstoreConfig struct {
	Bin          string `yaml:"bin" json:"bin"`                                   // default vecstore_server
	GRPCAddr     string `yaml:"grpc_addr" json:"grpc_addr"`                       // default 127.0.0.1:7100
	RocksDBPath  string `yaml:"rocksdb_path" json:"rocksdb_path"`                 // default <stratum data_dir>/vecstore_rocksdb
	HealthAddr   string `yaml:"health_addr" json:"health_addr"`                   // default 127.0.0.1:7101 (informational)
	ExtraArgsRaw string `yaml:"extra_args,omitempty" json:"extra_args,omitempty"` // extra CLI args (informational)
}

// EmbedConfig is the mock embed process parameters.
type EmbedConfig struct {
	Bin         string `yaml:"bin" json:"bin"`                   // default mock-embed
	ServiceAddr string `yaml:"service_addr" json:"service_addr"` // default http://localhost:8080
}

// StratumConfig mirrors cmd/stratum's fileConfig schema (plus raft
// timing) so the console can edit every parameter that affects startup.
type StratumConfig struct {
	Bin          string      `yaml:"bin" json:"bin"`                     // default stratum
	NodeID       int64       `yaml:"node_id" json:"node_id"`             // default 1
	DataDir      string      `yaml:"data_dir" json:"data_dir"`           // default ./run/data/node<ID>/stratum
	GRPCAddr     string      `yaml:"grpc_addr" json:"grpc_addr"`         // default 0.0.0.0:7000
	RaftAddr     string      `yaml:"raft_addr" json:"raft_addr"`         // default 0.0.0.0:8000
	Peers        []PeerEntry `yaml:"peers" json:"peers"`                 // raft peers
	VecstoreAddr string      `yaml:"vecstore_addr" json:"vecstore_addr"` // default 127.0.0.1:7100
	EmbedAddr    string      `yaml:"embed_addr" json:"embed_addr"`       // default http://localhost:8080

	// Raft timing (ms).
	HeartbeatIntervalMS  int64 `yaml:"heartbeat_interval_ms,omitempty" json:"heartbeat_interval_ms,omitempty"`     // default 200
	ElectionTimeoutMinMS int64 `yaml:"election_timeout_min_ms,omitempty" json:"election_timeout_min_ms,omitempty"` // default 2000
	ElectionTimeoutMaxMS int64 `yaml:"election_timeout_max_ms,omitempty" json:"election_timeout_max_ms,omitempty"` // default 4000

	// Index manager.
	IndexLRUCapacity         int `yaml:"index_lru_capacity,omitempty" json:"index_lru_capacity,omitempty"`
	IndexLoadWaitTimeoutMS   int `yaml:"index_load_wait_timeout_ms,omitempty" json:"index_load_wait_timeout_ms,omitempty"`
	IndexCallbackMaxRetries  int `yaml:"index_callback_max_retries,omitempty" json:"index_callback_max_retries,omitempty"`
	IndexCallbackRetryBaseMS int `yaml:"index_callback_retry_base_interval_ms,omitempty" json:"index_callback_retry_base_interval_ms,omitempty"`

	// Write / delete coordinators.
	WriteMaxRetries   int `yaml:"write_max_retries,omitempty" json:"write_max_retries,omitempty"`
	WriteRetryBaseMS  int `yaml:"write_retry_base_interval_ms,omitempty" json:"write_retry_base_interval_ms,omitempty"`
	DeleteMaxRetries  int `yaml:"delete_max_retries,omitempty" json:"delete_max_retries,omitempty"`
	DeleteRetryBaseMS int `yaml:"delete_retry_base_interval_ms,omitempty" json:"delete_retry_base_interval_ms,omitempty"`
}

// PeerEntry is one raft peer (same shape as cmd/stratum's raft.peers).
type PeerEntry struct {
	ID          int64  `yaml:"id" json:"id"`
	Addr        string `yaml:"addr" json:"addr"`
	ServiceAddr string `yaml:"service_addr,omitempty" json:"service_addr,omitempty"`
}

// ServiceID names the managed local services.
type ServiceID string

const (
	ServiceVecstore ServiceID = "vecstore"
	ServiceEmbed    ServiceID = "embed"
	ServiceStratum  ServiceID = "stratum"
)

// AllServices is the fixed management order.
var AllServices = []ServiceID{ServiceVecstore, ServiceEmbed, ServiceStratum}

// defaultOpsConfig returns the console defaults. Relative paths resolve
// against the working directory (matching the start.sh run/ layout).
func defaultOpsConfig(nodeID int) OpsConfig {
	return OpsConfig{
		NodeID:    nodeID,
		BinDir:    filepath.Join("run", "bin"),
		LogDir:    filepath.Join("run", "log"),
		ConfigDir: filepath.Join("run", "configs"),
		Cluster: []ClusterNode{
			{ID: nodeID, GatewayAddr: "http://127.0.0.1:8081"},
		},
		Docker: DockerClusterConfig{
			Enabled:         true,
			Script:          filepath.Join("scripts", "docker-cluster.sh"),
			Nodes:           3,
			BasePort:        17000,
			Network:         "stratum-net",
			Image:           "stratum-node:latest",
			ContainerPrefix: "stratum-node",
			WithEmbed:       true,
		},
		Services: ServiceConfigs{
			Vecstore: VecstoreConfig{
				Bin:         "vecstore_server",
				GRPCAddr:    "127.0.0.1:7100",
				RocksDBPath: "",
				HealthAddr:  "127.0.0.1:7101",
			},
			Embed: EmbedConfig{
				Bin:         "mock-embed",
				ServiceAddr: "http://localhost:8080",
			},
			Stratum: StratumConfig{
				Bin:          "stratum",
				NodeID:       int64(nodeID),
				DataDir:      filepath.Join("run", "data", fmt.Sprintf("node%d", nodeID), "stratum"),
				GRPCAddr:     "0.0.0.0:7000",
				RaftAddr:     "0.0.0.0:8000",
				Peers:        []PeerEntry{{ID: int64(nodeID), Addr: "localhost:8000", ServiceAddr: "localhost:7000"}},
				VecstoreAddr: "127.0.0.1:7100",
				EmbedAddr:    "http://localhost:8080",

				HeartbeatIntervalMS:  200,
				ElectionTimeoutMinMS: 2000,
				ElectionTimeoutMaxMS: 4000,

				IndexLRUCapacity:         16,
				IndexLoadWaitTimeoutMS:   5000,
				IndexCallbackMaxRetries:  3,
				IndexCallbackRetryBaseMS: 200,

				WriteMaxRetries:   3,
				WriteRetryBaseMS:  100,
				DeleteMaxRetries:  5,
				DeleteRetryBaseMS: 500,
			},
		},
	}
}

// loadOpsConfig reads the YAML file; a missing file yields the defaults.
// Values that are zero in the file (e.g. an unset bin) fall back to the
// defaults so hand-written minimal files work.
func loadOpsConfig(path string) (OpsConfig, error) {
	cfg := defaultOpsConfig(0)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}
	applyOpsDefaults(&cfg)
	return cfg, nil
}

// applyOpsDefaults fills empty fields with sensible defaults.
func applyOpsDefaults(cfg *OpsConfig) {
	d := defaultOpsConfig(cfg.NodeID)
	if cfg.BinDir == "" {
		cfg.BinDir = d.BinDir
	}
	if cfg.LogDir == "" {
		cfg.LogDir = d.LogDir
	}
	if cfg.ConfigDir == "" {
		cfg.ConfigDir = d.ConfigDir
	}
	if len(cfg.Cluster) == 0 {
		cfg.Cluster = []ClusterNode{{ID: cfg.NodeID, GatewayAddr: "http://127.0.0.1:8081"}}
	}

	dk := &cfg.Docker
	if dk.Script == "" {
		dk.Script = filepath.Join("scripts", "docker-cluster.sh")
	}
	if dk.Nodes <= 0 {
		dk.Nodes = 3
	}
	if dk.BasePort <= 0 {
		dk.BasePort = 17000
	}
	if dk.Network == "" {
		dk.Network = "stratum-net"
	}
	if dk.Image == "" {
		dk.Image = "stratum-node:latest"
	}
	if dk.ContainerPrefix == "" {
		dk.ContainerPrefix = "stratum-node"
	}
	_ = dk // WithEmbed 是布尔，无需默认值修正

	v := &cfg.Services.Vecstore
	if v.Bin == "" {
		v.Bin = "vecstore_server"
	}
	if v.GRPCAddr == "" {
		v.GRPCAddr = "127.0.0.1:7100"
	}
	if v.HealthAddr == "" {
		v.HealthAddr = "127.0.0.1:7101"
	}

	e := &cfg.Services.Embed
	if e.Bin == "" {
		e.Bin = "mock-embed"
	}
	if e.ServiceAddr == "" {
		e.ServiceAddr = "http://localhost:8080"
	}

	s := &cfg.Services.Stratum
	if s.Bin == "" {
		s.Bin = "stratum"
	}
	if s.NodeID == 0 {
		s.NodeID = int64(cfg.NodeID)
	}
	if s.DataDir == "" {
		s.DataDir = filepath.Join("run", "data", fmt.Sprintf("node%d", cfg.NodeID), "stratum")
	}
	if s.GRPCAddr == "" {
		s.GRPCAddr = "0.0.0.0:7000"
	}
	if s.RaftAddr == "" {
		s.RaftAddr = "0.0.0.0:8000"
	}
	if len(s.Peers) == 0 {
		s.Peers = []PeerEntry{{ID: s.NodeID, Addr: "localhost:8000", ServiceAddr: "localhost:7000"}}
	}
	if s.VecstoreAddr == "" {
		s.VecstoreAddr = "127.0.0.1:7100"
	}
	if s.EmbedAddr == "" {
		s.EmbedAddr = "http://localhost:8080"
	}
	if s.HeartbeatIntervalMS == 0 {
		s.HeartbeatIntervalMS = 200
	}
	if s.ElectionTimeoutMinMS == 0 {
		s.ElectionTimeoutMinMS = 2000
	}
	if s.ElectionTimeoutMaxMS == 0 {
		s.ElectionTimeoutMaxMS = 4000
	}
	if s.IndexLRUCapacity == 0 {
		s.IndexLRUCapacity = 16
	}
	if s.IndexLoadWaitTimeoutMS == 0 {
		s.IndexLoadWaitTimeoutMS = 5000
	}
	if s.IndexCallbackMaxRetries == 0 {
		s.IndexCallbackMaxRetries = 3
	}
	if s.IndexCallbackRetryBaseMS == 0 {
		s.IndexCallbackRetryBaseMS = 200
	}
	if s.WriteMaxRetries == 0 {
		s.WriteMaxRetries = 3
	}
	if s.WriteRetryBaseMS == 0 {
		s.WriteRetryBaseMS = 100
	}
	if s.DeleteMaxRetries == 0 {
		s.DeleteMaxRetries = 5
	}
	if s.DeleteRetryBaseMS == 0 {
		s.DeleteRetryBaseMS = 500
	}

	// vecstore rocksdb path defaults to the stratum data dir sibling.
	if v.RocksDBPath == "" {
		v.RocksDBPath = filepath.Join(s.DataDir, "vecstore_rocksdb")
	}
}

// saveOpsConfig writes the config as YAML (creating parent dirs).
func saveOpsConfig(path string, cfg *OpsConfig) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// binPath resolves the executable for a service, preferring the console
// bin dir, falling back to PATH if not found there.
func (o *OpsConfig) binPath(svc ServiceID) string {
	name := ""
	switch svc {
	case ServiceVecstore:
		name = o.Services.Vecstore.Bin
	case ServiceEmbed:
		name = o.Services.Embed.Bin
	case ServiceStratum:
		name = o.Services.Stratum.Bin
	}
	if name == "" {
		return ""
	}
	if filepath.IsAbs(name) {
		return name
	}
	p := filepath.Join(o.BinDir, name)
	if _, err := os.Stat(p); err == nil {
		return p
	}
	return name
}

// writeStratumConfig renders the stratum YAML config (cmd/stratum's
// fileConfig schema) into cfg.ConfigDir/node<N>.yaml and returns the
// path. The generated file is what gateway passes via `stratum -config`.
func (o *OpsConfig) writeStratumConfig() (string, error) {
	s := &o.Services.Stratum
	if err := os.MkdirAll(o.ConfigDir, 0o755); err != nil {
		return "", err
	}

	type peerFile struct {
		ID          int64  `yaml:"id"`
		Addr        string `yaml:"addr"`
		ServiceAddr string `yaml:"service_addr,omitempty"`
	}
	doc := struct {
		Node struct {
			NodeID   int64  `yaml:"node_id"`
			GRPCAddr string `yaml:"grpc_addr"`
			RaftAddr string `yaml:"raft_addr"`
		} `yaml:"node"`
		Raft struct {
			Peers                []peerFile `yaml:"peers"`
			HeartbeatIntervalMS  int64      `yaml:"heartbeat_interval_ms,omitempty"`
			ElectionTimeoutMinMS int64      `yaml:"election_timeout_min_ms,omitempty"`
			ElectionTimeoutMaxMS int64      `yaml:"election_timeout_max_ms,omitempty"`
		} `yaml:"raft"`
		Storage struct {
			DataDir string `yaml:"data_dir"`
		} `yaml:"storage"`
		Vecstore struct {
			GRPCAddr string `yaml:"grpc_addr"`
		} `yaml:"vecstore"`
		Embed struct {
			ServiceAddr string `yaml:"service_addr"`
		} `yaml:"embed"`
		IndexManager struct {
			LRUCapacity         int `yaml:"lru_capacity"`
			LoadWaitTimeoutMS   int `yaml:"load_wait_timeout_ms"`
			CallbackMaxRetries  int `yaml:"callback_max_retries"`
			CallbackRetryBaseMS int `yaml:"callback_retry_base_interval_ms"`
		} `yaml:"index_manager"`
		WriteCoordinator struct {
			MaxRetries          int `yaml:"max_retries"`
			RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
		} `yaml:"write_coordinator"`
		DeleteCoordinator struct {
			MaxRetries          int `yaml:"max_retries"`
			RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
		} `yaml:"delete_coordinator"`
	}{}

	doc.Node.NodeID = s.NodeID
	doc.Node.GRPCAddr = s.GRPCAddr
	doc.Node.RaftAddr = s.RaftAddr
	for _, p := range s.Peers {
		doc.Raft.Peers = append(doc.Raft.Peers, peerFile{ID: p.ID, Addr: p.Addr, ServiceAddr: p.ServiceAddr})
	}
	doc.Raft.HeartbeatIntervalMS = s.HeartbeatIntervalMS
	doc.Raft.ElectionTimeoutMinMS = s.ElectionTimeoutMinMS
	doc.Raft.ElectionTimeoutMaxMS = s.ElectionTimeoutMaxMS
	doc.Storage.DataDir = s.DataDir
	doc.Vecstore.GRPCAddr = s.VecstoreAddr
	doc.Embed.ServiceAddr = s.EmbedAddr
	doc.IndexManager.LRUCapacity = s.IndexLRUCapacity
	doc.IndexManager.LoadWaitTimeoutMS = s.IndexLoadWaitTimeoutMS
	doc.IndexManager.CallbackMaxRetries = s.IndexCallbackMaxRetries
	doc.IndexManager.CallbackRetryBaseMS = s.IndexCallbackRetryBaseMS
	doc.WriteCoordinator.MaxRetries = s.WriteMaxRetries
	doc.WriteCoordinator.RetryBaseIntervalMS = s.WriteRetryBaseMS
	doc.DeleteCoordinator.MaxRetries = s.DeleteMaxRetries
	doc.DeleteCoordinator.RetryBaseIntervalMS = s.DeleteRetryBaseMS

	data, err := yaml.Marshal(&doc)
	if err != nil {
		return "", err
	}
	path := filepath.Join(o.ConfigDir, fmt.Sprintf("node%d.yaml", cfgNodeID(*o)))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// cfgNodeID returns the effective node ID for file naming.
func cfgNodeID(o OpsConfig) int64 {
	if o.Services.Stratum.NodeID != 0 {
		return o.Services.Stratum.NodeID
	}
	return int64(o.NodeID)
}

// duration converts an ms int to time.Duration (used by tests).
func msToDuration(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
