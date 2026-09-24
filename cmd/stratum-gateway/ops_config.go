// Ops-config: the console (stratum-gateway) keeps its own YAML file
// (default ./run/console.yaml) holding the cluster node list plus the
// local service startup parameters. This lets the web console operate
// (inspect / start / stop / edit parameters) before the database itself
// is running.
//
// Path defaults are relative to the gateway's working directory so the
// one-click scripts/gateway.sh layout (run/bin, run/log, run/data) is reused as-is.
package main

import (
	"fmt"
	"os"
	"path/filepath"

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
	// 启用后控制台通过转调编排脚本 scripts/cluster.sh 管理节点生命周期。
	Docker DockerClusterConfig `yaml:"docker" json:"docker"`

	Services ServiceConfigs `yaml:"services" json:"services"`
}

// DockerClusterConfig 描述控制台管理的 docker 集群。所有参数都是集群级
// 统一配置：修改后重建整个集群，而不是单独改某个节点。
type DockerClusterConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Topology 选择控制台驱动的编排方式：""/"single" 为所有节点同构的单层集群，
	// "two-tier" 为控制层只持元数据、存储层只持数据的两层集群
	// （Stratum_设计文档v13.md §11 阶段 ④）。两者由同一个编排脚本
	// （scripts/cluster.sh）执行，靠 --topology 区分。
	//
	// 两者不可互换，控制台也不该假装可以：两层拓扑里一个节点要么是控制节点、
	// 要么是存储节点，页面因此分两组展示——把节点列表当成同一个池子，就会把
	// 读请求发给一个提供不了该服务的节点。
	Topology string `yaml:"topology" json:"topology"`

	Script          string `yaml:"script" json:"script"`                       // 单层编排脚本（scripts/cluster.sh）
	ScriptTwoTier   string `yaml:"script_two_tier" json:"script_two_tier"`     // 两层编排脚本（同一个 cluster.sh，靠 --topology 区分）
	Nodes           int    `yaml:"nodes" json:"nodes"`                         // 单层：集群节点数；两层：控制层节点数
	StorageNodes    int    `yaml:"storage_nodes" json:"storage_nodes"`         // 两层：存储层节点数
	BasePort        int    `yaml:"base_port" json:"base_port"`                 // 起始 gRPC 宿主端口（两层时指控制层）
	StorageBasePort int    `yaml:"storage_base_port" json:"storage_base_port"` // 两层：存储层起始 gRPC 宿主端口
	Network         string `yaml:"network" json:"network"`                     // Docker 网络名
	Image           string `yaml:"image" json:"image"`                         // stratum 镜像名
	ContainerPrefix string `yaml:"container_prefix" json:"container_prefix"`   // 容器名前缀（stratum-nodeN）
	WithEmbed       bool   `yaml:"with_embed" json:"with_embed"`               // 是否同时启动 mock-embed 依赖
}

// TopologyTwoTier 是 DockerClusterConfig.Topology 的两层取值。
const TopologyTwoTier = "two-tier"

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

	// IndexDir is the root the vecstore's on-disk index RPCs are confined to
	// (vecstore_server --index_dir). Empty means "use the stratum service's
	// data_dir", which is where the IndexManager writes its indexes; a deployment
	// that keeps them elsewhere sets this explicitly. Without a root the vecstore
	// would take whatever path a caller names (M4 of docs/code-review-2026-09-24.md).
	IndexDir string `yaml:"index_dir,omitempty" json:"index_dir,omitempty"`
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

	// StationSecret is the node's copy of the station's trust-mark key
	// (node.station_secret). scripts/gateway.sh fills it from run/station-secret,
	// the same file the station signs with; without it a node cannot verify the
	// station's forwards at all, and with require_authenticated on it refuses
	// every client-facing call (H4 of docs/code-review-2026-09-24.md).
	//
	// It travels into the generated node config, so it is a SECRET sitting in
	// run/console.yaml — which is why /ops refuses to overwrite it over HTTP
	// (see applyConfigPatch).
	StationSecret string `yaml:"station_secret,omitempty" json:"station_secret,omitempty"`

	// RequireAuthenticated mirrors node.require_authenticated in the generated
	// config. Pointer so "unset" (→ the node's own default, which follows
	// StationSecret) stays distinguishable from an explicit false.
	RequireAuthenticated *bool `yaml:"require_authenticated,omitempty" json:"require_authenticated,omitempty"`
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
// against the working directory (matching the scripts/gateway.sh run/ layout).
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
			Script:          filepath.Join("scripts", "cluster.sh"),
			ScriptTwoTier:   filepath.Join("scripts", "cluster.sh"),
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
	// 老配置里可能还写着 scripts/docker-cluster.sh / docker-cluster-both.sh
	// （合并成一个 cluster.sh 之前的名字）。就地迁移而不是让它去调用一个已经不存在
	// 的脚本：那种失败是「运维页的按钮没反应」，比一个配置项难懂得多。
	dk.Script = migrateClusterScript(dk.Script)
	dk.ScriptTwoTier = migrateClusterScript(dk.ScriptTwoTier)
	if dk.Script == "" {
		dk.Script = filepath.Join("scripts", "cluster.sh")
	}
	if dk.ScriptTwoTier == "" {
		dk.ScriptTwoTier = filepath.Join("scripts", "cluster.sh")
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
			// Both are written only when set: an unset RequireAuthenticated must
			// reach cmd/stratum as "unset" so its default can follow
			// StationSecret, which is the whole point of the pointer.
			RequireAuthenticated *bool  `yaml:"require_authenticated,omitempty"`
			StationSecret        string `yaml:"station_secret,omitempty"`
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
	doc.Node.RequireAuthenticated = s.RequireAuthenticated
	doc.Node.StationSecret = s.StationSecret
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

// migrateClusterScript 把合并前的编排脚本名换成统一的 scripts/cluster.sh。
//
// 合并前是两个脚本（scripts/docker-cluster.sh 单层、scripts/docker-cluster-both.sh
// 两层），合并后是同一个 scripts/cluster.sh + --topology。老 run/console.yaml 里仍
// 写着旧名，照用会去执行一个已经删除的文件——表面上只是控制台上那几个 docker 按钮
// 全失败，错误信息却说"文件不存在"，与哪个配置项有关看不出来。
//
// 只认这两个历史名字；别的路径一律原样保留：这个字段允许指向自定义脚本，不能替
// 用户改。
func migrateClusterScript(script string) string {
	switch filepath.Base(script) {
	case "docker-cluster.sh", "docker-cluster-both.sh":
		return filepath.Join(filepath.Dir(script), "cluster.sh")
	}
	return script
}
