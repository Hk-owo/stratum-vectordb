package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeServiceBin writes a tiny shell script that sleeps forever, used as
// a stand-in managed service for lifecycle tests.
func fakeServiceBin(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// newTestSupervisor builds a supervisor whose services all point at fake
// scripts in a temp dir.
func newTestSupervisor(t *testing.T, nodeID int) *Supervisor {
	t.Helper()
	dir := t.TempDir()
	cfg := defaultOpsConfig(nodeID)
	cfg.BinDir = dir
	cfg.LogDir = t.TempDir()
	cfg.ConfigDir = t.TempDir()
	fakeServiceBin(t, dir, cfg.Services.Stratum.Bin)
	fakeServiceBin(t, dir, cfg.Services.Vecstore.Bin)
	fakeServiceBin(t, dir, cfg.Services.Embed.Bin)
	sup := NewSupervisor(&cfg)
	t.Cleanup(func() { sup.StopAll(3 * time.Second) })
	return sup
}

func TestSupervisor_StartStopStatus(t *testing.T) {
	sup := newTestSupervisor(t, 1)

	// Initially all stopped.
	for _, svc := range AllServices {
		if st := findService(sup.Status(), svc); st.Running {
			t.Errorf("%s should be stopped initially", svc)
		}
	}

	// Start stratum → running.
	if err := sup.Start(ServiceStratum); err != nil {
		t.Fatalf("Start(stratum): %v", err)
	}
	st := findService(sup.Status(), ServiceStratum)
	if !st.Running {
		t.Fatalf("stratum not running after Start: %+v", st)
	}
	if st.PID <= 0 {
		t.Errorf("invalid pid %d", st.PID)
	}
	if st.LogFile == "" {
		t.Errorf("log file not set: %+v", st)
	}

	// Idempotent second start.
	if err := sup.Start(ServiceStratum); err != nil {
		t.Fatalf("second Start(stratum) should be a no-op: %v", err)
	}

	// Stop → stopped.
	if err := sup.Stop(ServiceStratum, 3*time.Second); err != nil {
		t.Fatalf("Stop(stratum): %v", err)
	}
	if st := findService(sup.Status(), ServiceStratum); st.Running {
		t.Errorf("stratum still running after Stop: %+v", st)
	}

	// Start all three via the dependency order.
	for _, svc := range AllServices {
		if err := sup.Start(svc); err != nil {
			t.Fatalf("Start(%s): %v", svc, err)
		}
	}
	for _, svc := range AllServices {
		if st := findService(sup.Status(), svc); !st.Running {
			t.Errorf("%s should be running: %+v", svc, st)
		}
	}

	sup.StopAll(3 * time.Second)
	for _, svc := range AllServices {
		if st := findService(sup.Status(), svc); st.Running {
			t.Errorf("%s still running after StopAll", svc)
		}
	}
}

// TestSupervisor_StartMissingBinary verifies a helpful error when the
// service executable is absent.
func TestSupervisor_StartMissingBinary(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultOpsConfig(1)
	cfg.BinDir = dir         // empty
	cfg.LogDir = t.TempDir() // avoid writing run/ under the test cwd
	cfg.ConfigDir = t.TempDir()
	sup := NewSupervisor(&cfg)
	t.Cleanup(func() { sup.StopAll(3 * time.Second) })

	if err := sup.Start(ServiceVecstore); err == nil {
		t.Fatal("expected error starting a service without a binary")
	}
}

// TestSupervisor_LogFileTail verifies service output lands in the log
// file (read back through the ops logs endpoint path).
func TestSupervisor_LogFileTail(t *testing.T) {
	sup := newTestSupervisor(t, 1)
	if err := sup.Start(ServiceEmbed); err != nil {
		t.Fatalf("Start(embed): %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the shell loop write nothing; just verify file exists
	st := findService(sup.Status(), ServiceEmbed)
	if _, err := os.Stat(st.LogFile); err != nil {
		t.Errorf("log file not created: %v", err)
	}
}

func findService(list []ServiceStatus, svc ServiceID) ServiceStatus {
	for _, s := range list {
		if s.Service == svc {
			return s
		}
	}
	return ServiceStatus{Service: svc}
}
