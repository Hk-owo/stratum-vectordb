package main

import (
	"strings"
	"testing"
)

// M4 of docs/code-review-2026-09-24.md: the vecstore confines Save/Load/… to the
// directories it was started with (--index_dir). The supervisor is what starts it in
// the single-machine shape, so the flag has to be there — the node writes its indexes
// under the stratum service's data_dir, and a vecstore without the flag would allow
// only the rocksdb path's parent and refuse every one of them.

// vecstoreArgs returns the argv the supervisor builds for the vecstore service.
func vecstoreArgs(t *testing.T, cfg *OpsConfig) []string {
	t.Helper()
	sup := NewSupervisor(cfg)
	cmd, err := sup.buildCmd(ServiceVecstore)
	if err != nil {
		t.Fatalf("buildCmd(vecstore): %v", err)
	}
	return cmd.Args
}

// newTestOpsConfig returns a console config whose service binaries exist, so
// buildCmd can resolve them (binPath stats the file).
func newTestOpsConfig(t *testing.T) OpsConfig {
	t.Helper()
	dir := t.TempDir()
	cfg := defaultOpsConfig(1)
	cfg.BinDir = dir
	cfg.LogDir = t.TempDir()
	cfg.ConfigDir = t.TempDir()
	fakeServiceBin(t, dir, cfg.Services.Stratum.Bin)
	fakeServiceBin(t, dir, cfg.Services.Vecstore.Bin)
	fakeServiceBin(t, dir, cfg.Services.Embed.Bin)
	return cfg
}

func argValue(args []string, prefix string) (string, bool) {
	for _, a := range args {
		if strings.HasPrefix(a, prefix+"=") {
			return strings.TrimPrefix(a, prefix+"="), true
		}
	}
	return "", false
}

func TestSupervisorPassesTheNodesDataDirAsTheIndexDir(t *testing.T) {
	cfg := newTestOpsConfig(t)
	cfg.Services.Stratum.DataDir = "/data/node1"

	got, ok := argValue(vecstoreArgs(t, &cfg), "--index_dir")
	if !ok {
		t.Fatal("the vecstore was started without --index_dir")
	}
	if got != "/data/node1" {
		t.Fatalf("--index_dir = %q, want the stratum service's data_dir", got)
	}
}

// An operator who keeps indexes somewhere else says so explicitly; that wins.
func TestSupervisorPrefersAnExplicitIndexDir(t *testing.T) {
	cfg := newTestOpsConfig(t)
	cfg.Services.Stratum.DataDir = "/data/node1"
	cfg.Services.Vecstore.IndexDir = "/indexes"

	got, _ := argValue(vecstoreArgs(t, &cfg), "--index_dir")
	if got != "/indexes" {
		t.Fatalf("--index_dir = %q, want the configured value", got)
	}
}

// With neither configured there is no flag to pass, and the vecstore falls back to
// the rocksdb path's parent — its own default, which the C++ side documents.
func TestSupervisorOmitsTheIndexDirWhenNeitherIsKnown(t *testing.T) {
	cfg := newTestOpsConfig(t)
	cfg.Services.Stratum.DataDir = ""
	cfg.Services.Vecstore.IndexDir = ""

	if _, ok := argValue(vecstoreArgs(t, &cfg), "--index_dir"); ok {
		t.Fatal("no index directory is known, so none may be passed")
	}
}
