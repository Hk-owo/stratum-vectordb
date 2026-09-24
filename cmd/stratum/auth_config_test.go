package main

// auth_config_test.go — H4 of docs/code-review-2026-09-24.md, the configuration half.
//
// The node now verifies the service station's trust mark with an HMAC under a shared
// secret, and a node that HOLDS that secret is, by that fact, part of a deployment
// with a station in front of it — which is the condition the client-facing gate exists
// for. So the secret turns the gate on. Getting this wrong is silent in both
// directions: too eager and a cluster refuses its own station's forwards, too lax and
// the gate stays off in exactly the deployment that configured a secret.

import "testing"

func TestLoadConfig_StationSecretTurnsTheAuthGateOn(t *testing.T) {
	cfg, err := loadConfig(writeConfigFile(t, "node:\n  station_secret: \"s3cret\"\n"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.StationSecret != "s3cret" {
		t.Fatalf("StationSecret = %q, want the configured secret", cfg.StationSecret)
	}
	if !cfg.RequireAuthenticated {
		t.Fatal("a configured station_secret must default require_authenticated on")
	}
	if cfg.RequireAuthenticatedSet {
		t.Fatal("the default must not masquerade as an explicit setting")
	}
}

// An explicit false wins: some deployments front a node with proxy-level access
// control and do not want a second gate.
func TestLoadConfig_ExplicitRequireAuthenticatedFalseWins(t *testing.T) {
	cfg, err := loadConfig(writeConfigFile(t,
		"node:\n  station_secret: \"s3cret\"\n  require_authenticated: false\n"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.RequireAuthenticated {
		t.Fatal("an explicit require_authenticated: false must be honoured")
	}
	if !cfg.RequireAuthenticatedSet {
		t.Fatal("an explicit setting must be recorded as such")
	}
}

// And without a secret nothing changes: a deployment that has not adopted a station
// keeps working exactly as before, with the gate off.
func TestLoadConfig_NoStationSecretLeavesTheGateAsConfigured(t *testing.T) {
	cfg, err := loadConfig(writeConfigFile(t, "node:\n  node_id: 7\n"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.RequireAuthenticated || cfg.RequireAuthenticatedSet {
		t.Fatalf("no secret configured: RequireAuthenticated=%v set=%v, want off and unset",
			cfg.RequireAuthenticated, cfg.RequireAuthenticatedSet)
	}

	// An explicit true is still honoured on its own.
	cfg, err = loadConfig(writeConfigFile(t, "node:\n  require_authenticated: true\n"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if !cfg.RequireAuthenticated || !cfg.RequireAuthenticatedSet {
		t.Fatalf("explicit true: RequireAuthenticated=%v set=%v, want true and set",
			cfg.RequireAuthenticated, cfg.RequireAuthenticatedSet)
	}
}
