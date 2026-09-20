package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoadConfig_GCCollectionKnobs pins the §8.6(d) config surface. These fields
// have no effect until they reach the IndexManagerConfig, and nothing fails loudly
// when they do not — a mistyped key would simply mean "collection never runs",
// which looks exactly like "collection found nothing to do".
func TestLoadConfig_GCCollectionKnobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "index_manager:\n" +
		"  gc_enabled: true\n" +
		"  serving_replica_min: 3\n" +
		"  graph_rebuild_ratio: 0.6\n" +
		"  gc_ratio_threshold: 0.35\n" +
		"  gc_sweep_interval_ms: 2500\n" +
		"  build_concurrency: 3\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if !cfg.IndexGCEnabled {
		t.Error("IndexGCEnabled = false, want true (gc_enabled: true)")
	}
	if cfg.IndexServingReplicaMin != 3 {
		t.Errorf("IndexServingReplicaMin = %d, want 3", cfg.IndexServingReplicaMin)
	}
	if cfg.IndexGCGraphRebuildRatio != 0.6 {
		t.Errorf("IndexGCGraphRebuildRatio = %v, want 0.6", cfg.IndexGCGraphRebuildRatio)
	}
	// The §8.6(d) scanner threshold is the one knob of the three that had no yaml
	// surface at all: it was reachable from IndexManagerConfig but not from a config
	// file, so a deployment could not separate it from append_max_dead_ratio — the
	// exact separation §G says is needed before §8.6(d) can observe anything.
	if cfg.IndexGCRatioThreshold != 0.35 {
		t.Errorf("IndexGCRatioThreshold = %v, want 0.35", cfg.IndexGCRatioThreshold)
	}
	if cfg.IndexGCSweepInterval != 2500*time.Millisecond {
		t.Errorf("IndexGCSweepInterval = %v, want 2.5s", cfg.IndexGCSweepInterval)
	}
	if cfg.IndexBuildConcurrency != 3 {
		t.Errorf("IndexBuildConcurrency = %d, want 3", cfg.IndexBuildConcurrency)
	}
}

// TestLoadConfig_GCCollectionIsOffUnlessAsked is the safety half: the default has
// to be "collect nothing", because collection rewrites an artifact that is
// currently serving queries. A config file written before the feature existed must
// not silently opt in.
func TestLoadConfig_GCCollectionIsOffUnlessAsked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("index_manager:\n  lru_capacity: 8\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.IndexGCEnabled {
		t.Error("IndexGCEnabled = true for a config that never mentioned it; collection must be opt-in")
	}
	// 0 means "take the IndexManager's default", which is what lets the default
	// live in one place (DefaultIndexServingReplicaMin / DefaultGCGraphRebuildRatio)
	// instead of being restated here.
	if cfg.IndexServingReplicaMin != 0 {
		t.Errorf("IndexServingReplicaMin = %d, want 0 (unset → IndexManager default)", cfg.IndexServingReplicaMin)
	}
	if cfg.IndexGCGraphRebuildRatio != 0 {
		t.Errorf("IndexGCGraphRebuildRatio = %v, want 0 (unset → IndexManager default)", cfg.IndexGCGraphRebuildRatio)
	}
}
