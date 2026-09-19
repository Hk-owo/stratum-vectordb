// docker_cluster.go — 控制台对 docker 集群的管理封装。
//
// 控制台（stratum-gateway）不直接调 docker CLI，而是转调编排脚本
// scripts/cluster.sh（统一入口，保证与命令行操作行为一致）。两种拓扑由同一个
// 脚本执行，靠 --topology single|two-tier 区分。
// 集群参数（节点数/端口/网络/镜像/embed）是集群级统一配置，不做单节点
// 差异化修改：修改参数后重建整个集群。
package main

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// newCmdContext 返回带超时的 context 及取消函数。
func newCmdContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// dockerCluster 封装对编排脚本（scripts/cluster.sh）的调用。
// 脚本路径来自集群配置（可以换成自定义脚本），所以这里不持有它。
type dockerCluster struct{}

// scriptPath 返回要驱动的编排脚本的绝对路径（相对路径按工作目录解析）。
//
// 两种拓扑现在由**同一个**脚本执行（scripts/cluster.sh），靠 --topology 区分；配置里
// 仍留着两个键，是为了让部署能分别指向自己的脚本。缺了就报错，而不是退回一个默认
// 路径：把集群管理交给一个运维没配置过的脚本，失败会出现在按钮上而不是配置上。
func (d *dockerCluster) scriptPath(cfg DockerClusterConfig) (string, error) {
	p := cfg.Script
	if cfg.Topology == TopologyTwoTier {
		p = cfg.ScriptTwoTier
		if p == "" {
			return "", fmt.Errorf("两层拓扑未配置编排脚本（ops config 的 docker.script_two_tier）")
		}
	} else if p == "" {
		return "", fmt.Errorf("未配置编排脚本（ops config 的 docker.script）")
	}
	if !filepath.IsAbs(p) {
		abs, err := filepath.Abs(p)
		if err != nil {
			return "", err
		}
		p = abs
	}
	return p, nil
}

// run 执行脚本命令，超时后终止；返回 stdout（合并 stderr 到错误信息）。
func (d *dockerCluster) run(timeout time.Duration, cfg DockerClusterConfig, args ...string) (string, error) {
	script, err := d.scriptPath(cfg)
	if err != nil {
		return "", err
	}
	ctx, cancel := newCmdContext(timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, script, args...)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err = cmd.Run()
	if err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return strings.TrimSpace(out.String()), fmt.Errorf("cluster %s: %s", strings.Join(args, " "), msg)
	}
	return strings.TrimSpace(out.String()), nil
}

// baseArgs 从集群统一配置组装公共选项。
//
// 第一项永远是 --topology：两种拓扑由同一个脚本执行，脚本靠它决定节点命名、端口
// 派生与配置生成方式。其余选项按拓扑分档——两层把「节点」分成控制组/存储组，所以
// 数量和端口也分两档。
func (d *dockerCluster) baseArgs(cfg DockerClusterConfig) []string {
	if cfg.Topology == TopologyTwoTier {
		args := []string{
			"--topology", "two-tier",
			"--control-base-port", fmt.Sprintf("%d", cfg.BasePort),
			"--network", cfg.Network,
		}
		// 两层节点必须自带 vecstore（镜像里带着 C++ 侧）。配置里的单层默认镜像
		// （stratum-node:latest）是 all-in-one 的，用它节点会起不来数据层，所以
		// 这里不传，让脚本用两层的默认镜像（stratum-storage:latest）。
		if cfg.Image != "" && cfg.Image != "stratum-node:latest" {
			args = append(args, "--image", cfg.Image)
		}
		if cfg.Nodes > 0 {
			args = append(args, "--control-nodes", fmt.Sprintf("%d", cfg.Nodes))
		}
		if cfg.StorageNodes > 0 {
			args = append(args, "--storage-nodes", fmt.Sprintf("%d", cfg.StorageNodes))
		}
		if cfg.StorageBasePort > 0 {
			args = append(args, "--storage-base-port", fmt.Sprintf("%d", cfg.StorageBasePort))
		}
		return args
	}
	return []string{
		"--topology", "single",
		"--base-port", fmt.Sprintf("%d", cfg.BasePort),
		"--network", cfg.Network,
		"--image", cfg.Image,
	}
}

// Status 返回集群 JSON 状态（脚本 status N --json 的原始输出）。
func (d *dockerCluster) Status(cfg DockerClusterConfig) ([]byte, error) {
	// 单层的 status 接受节点数位置参数；两层从选项读，多传一个位置参数没有意义，
	// 所以这里按拓扑分开组装。
	args := append(d.baseArgs(cfg), "status")
	if cfg.Topology != TopologyTwoTier {
		args = append(args, fmt.Sprintf("%d", cfg.Nodes))
	}
	args = append(args, "--json")
	out, err := d.run(30*time.Second, cfg, args...)
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// Up 启动（幂等）或重建整个集群。force=true 时按当前集群参数重建容器。
func (d *dockerCluster) Up(cfg DockerClusterConfig, force bool) (string, error) {
	args := append(d.baseArgs(cfg), "up")
	if cfg.Topology != TopologyTwoTier {
		args = append(args, fmt.Sprintf("%d", cfg.Nodes))
	}
	if cfg.WithEmbed {
		args = append(args, "--with-embed")
	}
	if force {
		args = append(args, "--force")
	}
	// up 会等待 leader 选举（最多 60s），给足超时。
	return d.run(180*time.Second, cfg, args...)
}

// Down 停止并删除节点容器（保留数据卷/网络/配置）。
func (d *dockerCluster) Down(cfg DockerClusterConfig) (string, error) {
	return d.run(120*time.Second, cfg, append(d.baseArgs(cfg), "down")...)
}

// Clean 完全清理（容器+数据卷+网络+配置）。
func (d *dockerCluster) Clean(cfg DockerClusterConfig) (string, error) {
	return d.run(120*time.Second, cfg, append(d.baseArgs(cfg), "clean")...)
}

// ensureInit 确保集群配置已生成（单节点启停依赖配置存在；init 幂等）。
func (d *dockerCluster) ensureInit(cfg DockerClusterConfig) error {
	args := append(d.baseArgs(cfg), "init")
	if cfg.Topology != TopologyTwoTier {
		args = append(args, fmt.Sprintf("%d", cfg.Nodes))
	}
	_, err := d.run(30*time.Second, cfg, args...)
	return err
}

// NodeStart 启动单个节点。
func (d *dockerCluster) NodeStart(cfg DockerClusterConfig, id int) (string, error) {
	if err := d.ensureInit(cfg); err != nil {
		return "", err
	}
	return d.run(60*time.Second, cfg, append(d.baseArgs(cfg),
		"start", fmt.Sprintf("%d", id))...)
}

// NodeStop 停止单个节点。
func (d *dockerCluster) NodeStop(cfg DockerClusterConfig, id int) (string, error) {
	return d.run(60*time.Second, cfg, append(d.baseArgs(cfg),
		"stop", fmt.Sprintf("%d", id))...)
}

// NodeRestart 重启单个节点。
func (d *dockerCluster) NodeRestart(cfg DockerClusterConfig, id int) (string, error) {
	if err := d.ensureInit(cfg); err != nil {
		return "", err
	}
	return d.run(90*time.Second, cfg, append(d.baseArgs(cfg),
		"restart", fmt.Sprintf("%d", id))...)
}

// NodeLogs 返回单个节点最近的日志文本。
func (d *dockerCluster) NodeLogs(cfg DockerClusterConfig, id int, lines int) (string, error) {
	if lines <= 0 || lines > 5000 {
		lines = 200
	}
	return d.run(30*time.Second, cfg, append(d.baseArgs(cfg),
		"logs", fmt.Sprintf("%d", id), "--lines", fmt.Sprintf("%d", lines))...)
}

// twoTierStorageIDBase 与编排脚本的 STORAGE_ID_BASE 对应：两层的存储节点从 11 起
// 编号（scripts/cluster.sh 的 storage_id）。它是 storage.nodes 的键，节点按 node_id
// 在那张表里解析自身地址，所以两边必须一致。
const twoTierStorageIDBase = 10

// dockerNodeIDValid 判断 id 是不是这个集群里的节点。两层拓扑的 id 有**两段**：
// 控制节点 1..Nodes，存储节点 11..(10+StorageNodes)。只认第一段，会让运维页对存储
// 节点的启停与日志按钮全部回 "invalid node id"——而它们恰恰是两层拓扑里干活的那批。
func dockerNodeIDValid(cfg DockerClusterConfig, id int) bool {
	if id >= 1 && id <= cfg.Nodes {
		return true
	}
	if cfg.Topology == TopologyTwoTier && cfg.StorageNodes > 0 {
		return id > twoTierStorageIDBase && id <= twoTierStorageIDBase+cfg.StorageNodes
	}
	return false
}

// dockerNodeIDHint 是 id 非法时的提示，说清这个集群认哪些 id。
func dockerNodeIDHint(cfg DockerClusterConfig) string {
	if cfg.Topology == TopologyTwoTier {
		storage := "无"
		if cfg.StorageNodes > 0 {
			storage = fmt.Sprintf("%d-%d",
				twoTierStorageIDBase+1, twoTierStorageIDBase+cfg.StorageNodes)
		}
		return fmt.Sprintf("invalid node id（控制节点 1-%d，存储节点 %s）", cfg.Nodes, storage)
	}
	return fmt.Sprintf("invalid node id（1-%d）", cfg.Nodes)
}
