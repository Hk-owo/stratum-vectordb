// docker_cluster.go — 控制台对 docker 集群的管理封装。
//
// 控制台（stratum-gateway）不直接调 docker CLI，而是转调
// scripts/docker-cluster.sh（统一入口，保证与命令行操作行为一致）。
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

// dockerCluster 封装对 docker-cluster.sh 的调用。
// dockerCluster 封装对编排脚本的调用。脚本路径来自集群配置（按拓扑选择），
// 所以这里不持有它。
type dockerCluster struct{}

// scriptPath 返回当前拓扑要驱动的编排脚本的绝对路径（相对路径按工作目录解析）。
//
// 两种拓扑各有一个脚本，且它们不是同一个脚本的两套参数：两层拓扑的节点分为
// 控制组与存储组，脚本本身不同。缺失时报错而不是退回单层脚本——把它交给一个
// 只认单层命令的脚本，会得到一个说"命令未知"的失败，比配置错误难懂得多。
func (d *dockerCluster) scriptPath(cfg DockerClusterConfig) (string, error) {
	p := cfg.Script
	if cfg.Topology == TopologyTwoTier {
		p = cfg.ScriptTwoTier
		if p == "" {
			return "", fmt.Errorf("两层拓扑未配置编排脚本（ops config 的 docker.script_two_tier）")
		}
	} else if p == "" {
		return "", fmt.Errorf("docker-cluster.sh 未配置（ops config 的 docker.script）")
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
		return strings.TrimSpace(out.String()), fmt.Errorf("docker-cluster %s: %s", strings.Join(args, " "), msg)
	}
	return strings.TrimSpace(out.String()), nil
}

// baseArgs 从集群统一配置组装公共选项。
//
// 两层拓扑把"节点"分成两档（控制组/存储组），所以选项也分成两档；单层拓扑保持
// 原样。两个脚本的选项形状是对齐的（见 scripts/docker-cluster-both.sh），因此这里
// 只有一处分支：加哪些选项。
func (d *dockerCluster) baseArgs(cfg DockerClusterConfig) []string {
	if cfg.Topology == TopologyTwoTier {
		args := []string{
			"--control-base-port", fmt.Sprintf("%d", cfg.BasePort),
			"--network", cfg.Network,
			"--image", cfg.Image,
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
