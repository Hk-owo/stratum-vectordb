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
type dockerCluster struct {
	script string // 脚本路径（相对或绝对）
}

// scriptPath 返回脚本绝对路径（相对路径按工作目录解析）。
func (d *dockerCluster) scriptPath() (string, error) {
	p := d.script
	if p == "" {
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
func (d *dockerCluster) run(timeout time.Duration, args ...string) (string, error) {
	script, err := d.scriptPath()
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

// baseArgs 从集群统一配置组装公共选项（--nodes/--base-port/--network/--image）。
func (d *dockerCluster) baseArgs(cfg DockerClusterConfig) []string {
	args := []string{
		"--base-port", fmt.Sprintf("%d", cfg.BasePort),
		"--network", cfg.Network,
		"--image", cfg.Image,
	}
	return args
}

// Status 返回集群 JSON 状态（脚本 status N --json 的原始输出）。
func (d *dockerCluster) Status(cfg DockerClusterConfig) ([]byte, error) {
	args := []string{"status", fmt.Sprintf("%d", cfg.Nodes), "--json"}
	out, err := d.run(30*time.Second, args...)
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// Up 启动（幂等）或重建整个集群。force=true 时按当前集群参数重建容器。
func (d *dockerCluster) Up(cfg DockerClusterConfig, force bool) (string, error) {
	args := append(d.baseArgs(cfg),
		"up", fmt.Sprintf("%d", cfg.Nodes),
	)
	if cfg.WithEmbed {
		args = append(args, "--with-embed")
	}
	if force {
		args = append(args, "--force")
	}
	// up 会等待 leader 选举（最多 60s），给足超时。
	return d.run(180*time.Second, args...)
}

// Down 停止并删除节点容器（保留数据卷/网络/配置）。
func (d *dockerCluster) Down(cfg DockerClusterConfig) (string, error) {
	return d.run(120*time.Second, append(d.baseArgs(cfg), "down")...)
}

// Clean 完全清理（容器+数据卷+网络+配置）。
func (d *dockerCluster) Clean(cfg DockerClusterConfig) (string, error) {
	return d.run(120*time.Second, append(d.baseArgs(cfg), "clean")...)
}

// ensureInit 确保集群配置已生成（单节点启停依赖配置存在；init 幂等）。
func (d *dockerCluster) ensureInit(cfg DockerClusterConfig) error {
	_, err := d.run(30*time.Second, append(d.baseArgs(cfg),
		"init", fmt.Sprintf("%d", cfg.Nodes))...)
	return err
}

// NodeStart 启动单个节点。
func (d *dockerCluster) NodeStart(cfg DockerClusterConfig, id int) (string, error) {
	if err := d.ensureInit(cfg); err != nil {
		return "", err
	}
	return d.run(60*time.Second, append(d.baseArgs(cfg),
		"start", fmt.Sprintf("%d", id))...)
}

// NodeStop 停止单个节点。
func (d *dockerCluster) NodeStop(cfg DockerClusterConfig, id int) (string, error) {
	return d.run(60*time.Second, append(d.baseArgs(cfg),
		"stop", fmt.Sprintf("%d", id))...)
}

// NodeRestart 重启单个节点。
func (d *dockerCluster) NodeRestart(cfg DockerClusterConfig, id int) (string, error) {
	if err := d.ensureInit(cfg); err != nil {
		return "", err
	}
	return d.run(90*time.Second, append(d.baseArgs(cfg),
		"restart", fmt.Sprintf("%d", id))...)
}

// NodeLogs 返回单个节点最近的日志文本。
func (d *dockerCluster) NodeLogs(cfg DockerClusterConfig, id int, lines int) (string, error) {
	if lines <= 0 || lines > 5000 {
		lines = 200
	}
	return d.run(30*time.Second, append(d.baseArgs(cfg),
		"logs", fmt.Sprintf("%d", id), "--lines", fmt.Sprintf("%d", lines))...)
}
