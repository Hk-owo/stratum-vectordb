// Supervisor manages the local service processes (vecstore / embed /
// stratum) that the console can start and stop before or after the
// database stack is running. The gateway itself is never managed here.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// ServiceStatus describes one managed service at a point in time.
type ServiceStatus struct {
	Service   ServiceID `json:"service"`
	Running   bool      `json:"running"`
	PID       int       `json:"pid,omitempty"`
	StartedAt string    `json:"started_at,omitempty"`
	LogFile   string    `json:"log_file"`
	LastError string    `json:"last_error,omitempty"`
}

// managedProc tracks one started process.
type managedProc struct {
	cmd       *exec.Cmd
	startedAt time.Time
	logFile   string
	lastError string
}

// Supervisor starts/stops the local managed services.
type Supervisor struct {
	mu    sync.Mutex
	cfg   *OpsConfig
	procs map[ServiceID]*managedProc
}

// NewSupervisor builds a Supervisor bound to a console config.
func NewSupervisor(cfg *OpsConfig) *Supervisor {
	return &Supervisor{
		cfg:   cfg,
		procs: make(map[ServiceID]*managedProc),
	}
}

// SetConfig swaps the config used for future starts (after a PUT
// /ops/config). Running processes are unaffected.
func (s *Supervisor) SetConfig(cfg *OpsConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
}

// Start launches one service. If it is already running the call is a
// no-op (idempotent). For stratum the startup YAML is (re)generated from
// the current config first, so parameter edits apply on the next start.
func (s *Supervisor) Start(svc ServiceID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.procs[svc]; ok && p.running() {
		return nil
	}

	cmd, err := s.buildCmd(svc)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.cfg.LogDir, 0o755); err != nil {
		return err
	}
	logPath := filepath.Join(s.cfg.LogDir, string(svc)+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd.Stdout = f
	cmd.Stderr = f
	// Run each service in its own process group so Stop can kill the
	// whole tree (children like stratum's embedded workers) reliably.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		f.Close()
		return fmt.Errorf("start %s: %w", svc, err)
	}
	s.procs[svc] = &managedProc{cmd: cmd, startedAt: time.Now(), logFile: logPath}
	return nil
}

// Stop terminates one service (SIGTERM to its process group, SIGKILL
// after a grace period) and reaps it.
func (s *Supervisor) Stop(svc ServiceID, grace time.Duration) error {
	s.mu.Lock()
	p, ok := s.procs[svc]
	if !ok {
		s.mu.Unlock()
		return nil // not running
	}
	pid := p.cmd.Process.Pid
	group := -pid
	s.mu.Unlock()

	if err := syscall.Kill(group, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		// Fall back to the single process if the group is gone.
		_ = p.cmd.Process.Kill()
	}
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(grace):
		_ = syscall.Kill(group, syscall.SIGKILL)
		<-done
	}

	s.mu.Lock()
	delete(s.procs, svc)
	s.mu.Unlock()
	return nil
}

// StopAll stops every managed service in reverse dependency order.
func (s *Supervisor) StopAll(grace time.Duration) {
	for i := len(AllServices) - 1; i >= 0; i-- {
		_ = s.Stop(AllServices[i], grace)
	}
}

// Status reports the current state of the three managed services.
func (s *Supervisor) Status() []ServiceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ServiceStatus, 0, len(AllServices))
	for _, svc := range AllServices {
		st := ServiceStatus{Service: svc, LogFile: filepath.Join(s.cfg.LogDir, string(svc)+".log")}
		if p, ok := s.procs[svc]; ok {
			if p.running() {
				st.Running = true
				st.PID = p.cmd.Process.Pid
				st.StartedAt = p.startedAt.Format(time.RFC3339)
				st.LogFile = p.logFile
			} else {
				st.LastError = "process exited"
			}
		}
		out = append(out, st)
	}
	return out
}

// running reports whether the underlying process is still alive.
func (p *managedProc) running() bool {
	if p.cmd == nil || p.cmd.Process == nil {
		return false
	}
	err := p.cmd.Process.Signal(syscall.Signal(0))
	return err == nil
}

// buildCmd constructs the exec.Cmd for a service from the console config.
func (s *Supervisor) buildCmd(svc ServiceID) (*exec.Cmd, error) {
	switch svc {
	case ServiceVecstore:
		v := &s.cfg.Services.Vecstore
		bin := s.cfg.binPath(ServiceVecstore)
		if bin == "" {
			return nil, fmt.Errorf("vecstore: no binary configured")
		}
		args := []string{
			"--rocksdb_path=" + v.RocksDBPath,
			"--grpc_addr=" + v.GRPCAddr,
		}
		if v.ExtraArgsRaw != "" {
			args = append(args, splitArgs(v.ExtraArgsRaw)...)
		}
		return exec.Command(bin, args...), nil

	case ServiceEmbed:
		bin := s.cfg.binPath(ServiceEmbed)
		if bin == "" {
			return nil, fmt.Errorf("embed: no binary configured")
		}
		return exec.Command(bin), nil

	case ServiceStratum:
		bin := s.cfg.binPath(ServiceStratum)
		if bin == "" {
			return nil, fmt.Errorf("stratum: no binary configured")
		}
		cfgPath, err := s.cfg.writeStratumConfig()
		if err != nil {
			return nil, fmt.Errorf("write stratum config: %w", err)
		}
		return exec.Command(bin, "-config", cfgPath), nil

	default:
		return nil, fmt.Errorf("unknown service %q", svc)
	}
}

// splitArgs splits a space-separated extra-args string (no quoting
// support; sufficient for simple informational flags).
func splitArgs(s string) []string {
	var out []string
	cur := ""
	inQuote := false
	for _, r := range s {
		switch {
		case r == '"' || r == '\'':
			inQuote = !inQuote
		case r == ' ' && !inQuote:
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
		default:
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
