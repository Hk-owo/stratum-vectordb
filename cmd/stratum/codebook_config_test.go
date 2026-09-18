package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfig_CodebookRefreshKnobs pins §3's config surface on the yaml path
// (docs/codebook-refresh-plan.md).
//
// Same reason the §8.6(d) knobs get their own test: these fields have no loud
// failure mode when they stop arriving — a mistyped key would simply mean "the
// codebook never gets refreshed", which looks exactly like "the codebook never
// drifted", and the whole mechanism would be off without anyone noticing.
func TestLoadConfig_CodebookRefreshKnobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "index_manager:\n" +
		"  max_codebook_drift_ratio: 0.4\n" +
		"  max_codebook_appends: 7\n" +
		"  min_codebook_baseline_vectors: 250\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.IndexMaxCodebookDriftRatio != 0.4 {
		t.Errorf("IndexMaxCodebookDriftRatio = %v, want 0.4", cfg.IndexMaxCodebookDriftRatio)
	}
	if cfg.IndexMaxCodebookAppends != 7 {
		t.Errorf("IndexMaxCodebookAppends = %d, want 7", cfg.IndexMaxCodebookAppends)
	}
	if cfg.IndexMinCodebookBaselineVectors != 250 {
		t.Errorf("IndexMinCodebookBaselineVectors = %d, want 250", cfg.IndexMinCodebookBaselineVectors)
	}
}

// TestLoadConfig_CodebookRefreshKnobsUnset pins that an unset key stays 0, which
// the IndexManager reads as "use my own default" (0.25 / 50). A config file that
// predates §3 must not end up with a zero threshold, which would fire a rebuild
// on every single version.
func TestLoadConfig_CodebookRefreshKnobsUnset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("index_manager: {}\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.IndexMaxCodebookDriftRatio != 0 {
		t.Errorf("IndexMaxCodebookDriftRatio = %v, want 0 (→ IndexManager default)",
			cfg.IndexMaxCodebookDriftRatio)
	}
	if cfg.IndexMaxCodebookAppends != 0 {
		t.Errorf("IndexMaxCodebookAppends = %d, want 0 (→ IndexManager default)",
			cfg.IndexMaxCodebookAppends)
	}
	if cfg.IndexMinCodebookBaselineVectors != 0 {
		t.Errorf("IndexMinCodebookBaselineVectors = %d, want 0 (→ IndexManager default)",
			cfg.IndexMinCodebookBaselineVectors)
	}
}
