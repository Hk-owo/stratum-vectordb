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
//
// done and waitErr belong to the single reaper goroutine Start spawns (see reap).
// Keeping them here is what makes running() a fact rather than a guess: the old
// check was "signal 0 succeeds", which a ZOMBIE also satisfies, so a service that
// crashed kept reporting running:true — and Start, believing it was already up,
// became a no-op forever (H11 of docs/code-review-2026-10-18.md).
type managedProc struct {
	cmd       *exec.Cmd
	startedAt time.Time
	logFile   string

	// logHandle is owned by the reaper: it closes it once the process is gone, so a
	// start/restart cycle does not leak a descriptor.
	logHandle *os.File

	// done is closed by the reaper after Wait returns. Running() reads it, Stop waits
	// on it, and nobody else calls Wait — a second Wait on the same process returns
	// "waitid: no child processes".
	done chan struct{}
	// waitErr is written once by the reaper, under s.mu, before done closes.
	waitErr error
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
	p := &managedProc{
		cmd:       cmd,
		startedAt: time.Now(),
		logFile:   logPath,
		logHandle: f,
		done:      make(chan struct{}),
	}
	s.procs[svc] = p
	go s.reap(svc, p)
	return nil
}

// reap is the ONLY caller of cmd.Wait: it turns the child's exit into state instead
// of leaving a zombie behind.
//
// Without it the process table grew by one unreaped child per crash, `kill(pid, 0)`
// kept succeeding on those zombies, and the console went on reporting a crashed
// service as running with the PID of a corpse.
func (s *Supervisor) reap(svc ServiceID, p *managedProc) {
	err := p.cmd.Wait()
	s.mu.Lock()
	if p.logHandle != nil {
		_ = p.logHandle.Close()
		p.logHandle = nil
	}
	p.waitErr = err
	s.mu.Unlock()
	close(p.done)
}

// Stop terminates one service (SIGTERM to its process group, SIGKILL after a grace
// period) and waits for the reaper to finish.
//
// It does not call Wait itself: the reaper owns that call, and two of them racing on
// one process meant one got "waitid: no child processes" — the failure mode of two
// concurrent POST /ops/stop, which could also leave Stop reporting success while the
// service was still up.
func (s *Supervisor) Stop(svc ServiceID, grace time.Duration) error {
	s.mu.Lock()
	p, ok := s.procs[svc]
	if !ok || p.cmd == nil || p.cmd.Process == nil {
		s.mu.Unlock()
		return nil // not running
	}
	if !p.running() {
		s.mu.Unlock()
		return nil // already exited; the reaper has recorded why
	}
	pid := p.cmd.Process.Pid
	group := -pid
	done := p.done
	s.mu.Unlock()

	if err := syscall.Kill(group, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		// Fall back to the single process if the group is gone.
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-done:
	case <-time.After(grace):
		_ = syscall.Kill(group, syscall.SIGKILL)
		<-done
	}
	// The entry stays: Status reads the exit reason out of it, and Start replaces it
	// when the service is started again.
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
				st.LogFile = p.logFile
				// Say WHY, not just that: a supervisor whose only output on a crashed
				// service is "process exited" leaves the operator to find the log and
				// guess.
				st.LastError = "process exited"
				if p.waitErr != nil {
					st.LastError = "process exited: " + p.waitErr.Error()
				}
			}
		}
		out = append(out, st)
	}
	return out
}

// running reports whether the underlying process is still alive.
//
// It reads the reaper's channel rather than probing the pid: signal 0 succeeds for a
// zombie, so the probe answered "alive" for every unreaped child and the console
// showed crashed services as running.
func (p *managedProc) running() bool {
	if p.done == nil {
		// Built without a reaper (a test fixture assembling the struct directly):
		// fall back to the probe rather than claiming the process is dead.
		if p.cmd == nil || p.cmd.Process == nil {
			return false
		}
		return p.cmd.Process.Signal(syscall.Signal(0)) == nil
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
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
		// The on-disk index RPCs (Save / Load / LoadForAppend / ExistsIndex) take a
		// path from the wire, so the vecstore confines them to --index_dir (M4 of
		// docs/code-review-2026-09-24.md). An explicit index_dir wins when a
		// deployment keeps indexes elsewhere.
		//
		// The default is <data_dir>/index, NOT <data_dir> (C3 of
		// docs/code-review-2026-10-18.md). The IndexManager keeps every index under
		// <data_dir>/index/<kbID>/, which is the set these RPCs are supposed to reach;
		// handing over the whole data directory puts RocksDB's own files, the document
		// store and the metadata stores inside the writable set, and Save() writes
		// whatever path it is handed — through a rename, so it replaces the target.
		// One Save() naming <data_dir>/vecstore_rocksdb/CURRENT ends the chunk store.
		indexDir := v.IndexDir
		if indexDir == "" && s.cfg.Services.Stratum.DataDir != "" {
			indexDir = filepath.Join(s.cfg.Services.Stratum.DataDir, "index")
		}
		if indexDir != "" {
			args = append(args, "--index_dir="+indexDir)
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
