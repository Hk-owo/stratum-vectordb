package main

// tuning_config_test.go — the config surface added when the numbers that were
// hardcoded in the components (or in this file) became keys.
//
// The failure mode is silence in both directions, which is why this file exists:
//
//   - a mistyped key parses as "absent" and leaves the default in place, so the
//     knob looks configured and does nothing;
//   - a key wired to the wrong field is invisible until someone measures the
//     wrong bound;
//   - a new field that nobody extended loadConfig() for stays at its zero value
//     for every config file, forever.
//
// The last one is what the reflection test guards: every field named in
// tuningFields must be zero for an empty config, so a field that acquires a
// non-zero default (or is dropped from the "unset = component default"
// convention) fails loudly.

import (
	"reflect"
	"testing"
	"time"
)

// tuningFields names the appConfig fields that follow the "zero = the
// component's own default" convention. A field added to the group must be named
// here too — the tests below use this list as the definition of the group.
//
// The process-level knobs (embed timeout, crash replay, epoch report, shutdown)
// are NOT in this list: nothing but this file uses them, so their defaults live
// in defaultConfig() and are pinned by the last test instead.
var tuningFields = []string{
	"StationMarkTTL", "RaftRPCTimeout", "RaftProposeApplyTimeout",
	"VecstoreKeepaliveInterval", "VecstoreKeepaliveTimeout",
	"PullIdleTimeout", "PullMaxDuration", "MaxInFlightWrites",
	"CursorWriteTimeout", "CursorRetryDelay",
	"ConfirmAttemptTimeout", "ConfirmRetryBackoff", "ConfirmBudget",
	"ReplicaPushTimeout", "PushTimeoutPerDoc", "PeerCursorTimeout",
	"TakeoverTimeout", "TakeoverCheckTimeout", "TakeoverMaxAttempts",
	"TakeoverMaxRetryDelay", "TerminalReclaimInterval", "TerminalReclaimAttempts",
	"SyncHeartbeatInterval", "PullDialTimeout",
	"HoldersCacheTTL", "HoldersCacheLimit",
	"CandidateTimeout", "CandidateTimeoutPerDoc", "MaxCandidateTimeout",
	"HealthDemoteAfter", "HealthDemoteFor",
	"ReconcileChainMinAge", "ReconcileDeletionPages",
	"DeletedVersionRetryInterval", "TombstoneGrace",
	"IndexBruteForceMaxChunks", "IndexBuildAbandonSweepInterval",
	"IndexBuildRetryTimeout", "IndexBuildRetryInterval",
	"IndexAccessPersistInterval",
	"WriteDispatchTimeout", "WriteDispatchClaimWindow",
	"WriteAbandonReportTimeout", "WriteMaxConcurrentDocs",
	"DeletingVersionSweepInterval", "DeletingKBSweepInterval",
	"LagCatchupTimeout", "LagCatchupHoldersRefresh",
	"GCWALReclaimInterval",
}

// TestLoadConfig_TuningKnobs parses every new key and checks it lands on its own
// field. One case per key, all in one file, because the whole point is that they
// are configured the same way.
func TestLoadConfig_TuningKnobs(t *testing.T) {
	cfg, err := loadConfig(writeConfigFile(t, `
node:
  station_mark_ttl_ms: 1000
raft:
  rpc_timeout_ms: 2000
  propose_apply_timeout_ms: 3000
vecstore:
  keepalive_interval_ms: 4000
  keepalive_timeout_ms: 5000
embed:
  timeout_ms: 6000
data_plane:
  pull_idle_timeout_ms: 7000
  pull_max_duration_ms: 8000
  max_in_flight_writes: 9
  cursor_write_timeout_ms: 10000
  cursor_retry_delay_ms: 11000
  confirm_attempt_timeout_ms: 12000
  confirm_retry_backoff_ms: 13000
  confirm_budget_ms: 14000
  replica_push_timeout_ms: 15000
  push_timeout_per_doc_ms: 16000
  peer_cursor_timeout_ms: 17000
  takeover_timeout_ms: 18000
  takeover_check_timeout_ms: 19000
  takeover_max_attempts: 20
  takeover_max_retry_delay_ms: 21000
  terminal_reclaim_interval_ms: 22000
  terminal_reclaim_attempts: 23
sync:
  heartbeat_interval_ms: 24000
  pull_dial_timeout_ms: 25000
  holders_cache_ttl_ms: 26000
  holders_cache_limit: 27
dispatcher:
  candidate_timeout_ms: 28000
  candidate_timeout_per_doc_ms: 29000
  max_candidate_timeout_ms: 30000
health:
  demote_after: 31
  demote_for_ms: 32000
reconcile:
  chain_reconcile_min_age_ms: 33000
  deletion_pages_per_sweep: 34
  deleted_version_retry_interval_ms: 35000
  tombstone_grace_ms: 36000
index_manager:
  brute_force_max_chunks: 37
  build_abandon_sweep_interval_ms: 38000
  build_retry_timeout_ms: 39000
  build_retry_interval_ms: 40000
  access_persist_interval_ms: 41000
write_coordinator:
  dispatch_timeout_ms: 42000
  dispatch_claim_window_ms: 43000
  abandon_report_timeout_ms: 44000
  max_concurrent_document_writes: 45
delete_coordinator:
  deleting_version_sweep_interval_ms: 46000
  deleting_kb_sweep_interval_ms: 47000
lag_catchup:
  timeout_ms: 48000
  holders_refresh_interval_ms: 49000
gc:
  wal_reclaim_interval_s: 50
recovery:
  crash_replay_max_attempts: 51
  crash_replay_retry_base_ms: 52
  epoch_report_retry_interval_ms: 53000
  epoch_report_retry_window_ms: 54000
  shutdown_timeout_ms: 55000
`))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"StationMarkTTL", cfg.StationMarkTTL, ms(1000)},
		{"RaftRPCTimeout", cfg.RaftRPCTimeout, ms(2000)},
		{"RaftProposeApplyTimeout", cfg.RaftProposeApplyTimeout, ms(3000)},
		{"VecstoreKeepaliveInterval", cfg.VecstoreKeepaliveInterval, ms(4000)},
		{"VecstoreKeepaliveTimeout", cfg.VecstoreKeepaliveTimeout, ms(5000)},
		{"EmbedTimeout", cfg.EmbedTimeout, ms(6000)},
		{"PullIdleTimeout", cfg.PullIdleTimeout, ms(7000)},
		{"PullMaxDuration", cfg.PullMaxDuration, ms(8000)},
		{"MaxInFlightWrites", cfg.MaxInFlightWrites, 9},
		{"CursorWriteTimeout", cfg.CursorWriteTimeout, ms(10000)},
		{"CursorRetryDelay", cfg.CursorRetryDelay, ms(11000)},
		{"ConfirmAttemptTimeout", cfg.ConfirmAttemptTimeout, ms(12000)},
		{"ConfirmRetryBackoff", cfg.ConfirmRetryBackoff, ms(13000)},
		{"ConfirmBudget", cfg.ConfirmBudget, ms(14000)},
		{"ReplicaPushTimeout", cfg.ReplicaPushTimeout, ms(15000)},
		{"PushTimeoutPerDoc", cfg.PushTimeoutPerDoc, ms(16000)},
		{"PeerCursorTimeout", cfg.PeerCursorTimeout, ms(17000)},
		{"TakeoverTimeout", cfg.TakeoverTimeout, ms(18000)},
		{"TakeoverCheckTimeout", cfg.TakeoverCheckTimeout, ms(19000)},
		{"TakeoverMaxAttempts", cfg.TakeoverMaxAttempts, 20},
		{"TakeoverMaxRetryDelay", cfg.TakeoverMaxRetryDelay, ms(21000)},
		{"TerminalReclaimInterval", cfg.TerminalReclaimInterval, ms(22000)},
		{"TerminalReclaimAttempts", cfg.TerminalReclaimAttempts, 23},
		{"SyncHeartbeatInterval", cfg.SyncHeartbeatInterval, ms(24000)},
		{"PullDialTimeout", cfg.PullDialTimeout, ms(25000)},
		{"HoldersCacheTTL", cfg.HoldersCacheTTL, ms(26000)},
		{"HoldersCacheLimit", cfg.HoldersCacheLimit, 27},
		{"CandidateTimeout", cfg.CandidateTimeout, ms(28000)},
		{"CandidateTimeoutPerDoc", cfg.CandidateTimeoutPerDoc, ms(29000)},
		{"MaxCandidateTimeout", cfg.MaxCandidateTimeout, ms(30000)},
		{"HealthDemoteAfter", cfg.HealthDemoteAfter, 31},
		{"HealthDemoteFor", cfg.HealthDemoteFor, ms(32000)},
		{"ReconcileChainMinAge", cfg.ReconcileChainMinAge, ms(33000)},
		{"ReconcileDeletionPages", cfg.ReconcileDeletionPages, 34},
		{"DeletedVersionRetryInterval", cfg.DeletedVersionRetryInterval, ms(35000)},
		{"TombstoneGrace", cfg.TombstoneGrace, ms(36000)},
		{"IndexBruteForceMaxChunks", cfg.IndexBruteForceMaxChunks, 37},
		{"IndexBuildAbandonSweepInterval", cfg.IndexBuildAbandonSweepInterval, ms(38000)},
		{"IndexBuildRetryTimeout", cfg.IndexBuildRetryTimeout, ms(39000)},
		{"IndexBuildRetryInterval", cfg.IndexBuildRetryInterval, ms(40000)},
		{"IndexAccessPersistInterval", cfg.IndexAccessPersistInterval, ms(41000)},
		{"WriteDispatchTimeout", cfg.WriteDispatchTimeout, ms(42000)},
		{"WriteDispatchClaimWindow", cfg.WriteDispatchClaimWindow, ms(43000)},
		{"WriteAbandonReportTimeout", cfg.WriteAbandonReportTimeout, ms(44000)},
		{"WriteMaxConcurrentDocs", cfg.WriteMaxConcurrentDocs, 45},
		{"DeletingVersionSweepInterval", cfg.DeletingVersionSweepInterval, ms(46000)},
		{"DeletingKBSweepInterval", cfg.DeletingKBSweepInterval, ms(47000)},
		{"LagCatchupTimeout", cfg.LagCatchupTimeout, ms(48000)},
		{"LagCatchupHoldersRefresh", cfg.LagCatchupHoldersRefresh, ms(49000)},
		{"GCWALReclaimInterval", cfg.GCWALReclaimInterval, 50 * time.Second},
		{"CrashReplayMaxAttempts", cfg.CrashReplayMaxAttempts, 51},
		{"CrashReplayRetryBaseMS", cfg.CrashReplayRetryBaseMS, 52},
		{"EpochReportRetryInterval", cfg.EpochReportRetryInterval, ms(53000)},
		{"EpochReportRetryWindow", cfg.EpochReportRetryWindow, ms(54000)},
		{"ShutdownTimeout", cfg.ShutdownTimeout, ms(55000)},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestLoadConfig_TuningKnobsUnsetStayAtTheComponentDefault is the safety half:
// an absent key must keep 0 ("the component decides"), not turn into a zero
// duration that fires a bound immediately.
func TestLoadConfig_TuningKnobsUnsetStayAtTheComponentDefault(t *testing.T) {
	cfg, err := loadConfig(writeConfigFile(t, "node:\n  node_id: 3\n"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	v := reflect.ValueOf(cfg)
	typ := v.Type()
	for _, name := range tuningFields {
		f, ok := typ.FieldByName(name)
		if !ok {
			t.Errorf("tuningFields names %s, which appConfig does not have", name)
			continue
		}
		if !v.FieldByIndex(f.Index).IsZero() {
			t.Errorf("%s = %v for a config that never mentioned it, want the zero value "+
				"(0 = the component's own default)",
				name, v.FieldByIndex(f.Index).Interface())
		}
	}
}

// TestDefaultConfig_ProcessLevelTuningDefaults pins the handful of knobs whose
// default lives in this file rather than in a component — nothing else owns them,
// so a zero here would mean "no timeout" (or "no retries") for an unconfigured
// node.
func TestDefaultConfig_ProcessLevelTuningDefaults(t *testing.T) {
	cfg := defaultConfig()

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"EmbedTimeout", cfg.EmbedTimeout, 30 * time.Second},
		{"CrashReplayMaxAttempts", cfg.CrashReplayMaxAttempts, crashReplayMaxAttempts},
		{"CrashReplayRetryBaseMS", cfg.CrashReplayRetryBaseMS, crashReplayRetryBaseMillis},
		{"EpochReportRetryInterval", cfg.EpochReportRetryInterval, epochReportRetryInterval},
		{"EpochReportRetryWindow", cfg.EpochReportRetryWindow, epochReportRetryWindow},
		{"ShutdownTimeout", cfg.ShutdownTimeout, 2 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}
