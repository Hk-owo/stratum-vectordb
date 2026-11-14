package plane

// timing_config_test.go — the "configured value wins, zero takes the default"
// convention the plane's timing knobs share.
//
// The failure mode this pins: a zero duration is not a small bound, it is one
// that fires immediately (an instant commit/confirm/pull timeout), so "unset"
// must never reach the loop as 0. The other half — a configured value actually
// winning — is what makes the yaml keys mean anything at all.

import (
	"testing"
	"time"
)

func TestLocalDataPlane_TimingKnobsDefaultWhenUnset(t *testing.T) {
	d := NewLocalDataPlane(LocalDataPlaneConfig{})

	checks := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"cursorWriteTimeout", d.cursorWriteTimeout, cursorWriteTimeout},
		{"cursorRetryDelay", d.cursorRetryDelay, cursorRetryDelay},
		{"confirmAttemptTimeout", d.confirmAttemptTimeout, confirmAttemptTimeout},
		{"confirmRetryBackoff", d.confirmRetryBackoff, confirmRetryBackoff},
		{"confirmBudget", d.confirmBudget, confirmBroadcastBudget},
		{"replicaPushTimeout", d.replicaPushTimeout, replicaPushTimeout},
		{"pushTimeoutPerDoc", d.pushTimeoutPerDoc, pushTimeoutPerDoc},
		{"peerCursorTimeout", d.peerCursorTimeout, peerCursorTimeout},
		{"takeoverTimeout", d.takeoverTimeout, takeoverTimeout},
		{"takeoverCheckTimeout", d.takeoverCheckTimeout, takeoverCheckTimeout},
		{"takeoverMaxRetryDelay", d.takeoverMaxRetryDelay, maxTakeoverRetryDelay},
		{"terminalReclaimEvery", d.terminalReclaimEvery, terminalReclaimInterval},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want the package default %v", c.name, c.got, c.want)
		}
	}
	if d.takeoverMaxAttempts != maxTakeoverAttempts {
		t.Errorf("takeoverMaxAttempts = %d, want the default %d", d.takeoverMaxAttempts, maxTakeoverAttempts)
	}
	if d.terminalReclaimTries != terminalReclaimAttempts {
		t.Errorf("terminalReclaimTries = %d, want the default %d", d.terminalReclaimTries, terminalReclaimAttempts)
	}
}

func TestLocalDataPlane_TimingKnobsTakeTheConfiguredValue(t *testing.T) {
	d := NewLocalDataPlane(LocalDataPlaneConfig{
		CursorWriteTimeout:      111 * time.Millisecond,
		CursorRetryDelay:        222 * time.Millisecond,
		ConfirmAttemptTimeout:   333 * time.Millisecond,
		ConfirmRetryBackoff:     444 * time.Millisecond,
		ConfirmBudget:           555 * time.Millisecond,
		ReplicaPushTimeout:      666 * time.Millisecond,
		PushTimeoutPerDoc:       777 * time.Millisecond,
		PeerCursorTimeout:       888 * time.Millisecond,
		TakeoverTimeout:         999 * time.Millisecond,
		TakeoverCheckTimeout:    1 * time.Second,
		TakeoverMaxAttempts:     7,
		TakeoverMaxRetryDelay:   2 * time.Second,
		TerminalReclaimInterval: 3 * time.Second,
		TerminalReclaimAttempts: 4,
	})

	checks := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"cursorWriteTimeout", d.cursorWriteTimeout, 111 * time.Millisecond},
		{"cursorRetryDelay", d.cursorRetryDelay, 222 * time.Millisecond},
		{"confirmAttemptTimeout", d.confirmAttemptTimeout, 333 * time.Millisecond},
		{"confirmRetryBackoff", d.confirmRetryBackoff, 444 * time.Millisecond},
		{"confirmBudget", d.confirmBudget, 555 * time.Millisecond},
		{"replicaPushTimeout", d.replicaPushTimeout, 666 * time.Millisecond},
		{"pushTimeoutPerDoc", d.pushTimeoutPerDoc, 777 * time.Millisecond},
		{"peerCursorTimeout", d.peerCursorTimeout, 888 * time.Millisecond},
		{"takeoverTimeout", d.takeoverTimeout, 999 * time.Millisecond},
		{"takeoverCheckTimeout", d.takeoverCheckTimeout, 1 * time.Second},
		{"takeoverMaxRetryDelay", d.takeoverMaxRetryDelay, 2 * time.Second},
		{"terminalReclaimEvery", d.terminalReclaimEvery, 3 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want the configured %v", c.name, c.got, c.want)
		}
	}
	if d.takeoverMaxAttempts != 7 {
		t.Errorf("takeoverMaxAttempts = %d, want 7", d.takeoverMaxAttempts)
	}
	if d.terminalReclaimTries != 4 {
		t.Errorf("terminalReclaimTries = %d, want 4", d.terminalReclaimTries)
	}
}

// TestCoordinatorDispatcher_CandidateBudgetFromConfig pins the scaled budget:
// floor + per-document allowance, capped — all three configurable, because the
// right numbers depend on the batch sizes a deployment actually writes.
func TestCoordinatorDispatcher_CandidateBudgetFromConfig(t *testing.T) {
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		CandidateTimeout:       10 * time.Second,
		CandidateTimeoutPerDoc: 100 * time.Millisecond,
		MaxCandidateTimeout:    12 * time.Second,
	})

	if got := d.candidateBudget(0); got != 10*time.Second {
		t.Errorf("candidateBudget(0) = %v, want the configured floor 10s", got)
	}
	if got := d.candidateBudget(10); got != 11*time.Second {
		t.Errorf("candidateBudget(10) = %v, want 10s + 10*100ms", got)
	}
	if got := d.candidateBudget(1000); got != 12*time.Second {
		t.Errorf("candidateBudget(1000) = %v, want the configured cap 12s", got)
	}

	// And the unset case still uses the package defaults.
	def := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{})
	if got := def.candidateBudget(0); got != defaultCandidateTimeout {
		t.Errorf("default candidateBudget(0) = %v, want %v", got, defaultCandidateTimeout)
	}
}

// TestLagCatchup_TimeoutFromConfig pins that a configured catch-up budget reaches
// the context it bounds, instead of the package constant.
func TestLagCatchup_TimeoutFromConfig(t *testing.T) {
	l := NewLagCatchup(LagCatchupConfig{
		Timeout:                42 * time.Second,
		HoldersRefreshInterval: 43 * time.Second,
	})
	if l.cfg.Timeout != 42*time.Second {
		t.Errorf("Timeout = %v, want 42s", l.cfg.Timeout)
	}
	if l.cfg.HoldersRefreshInterval != 43*time.Second {
		t.Errorf("HoldersRefreshInterval = %v, want 43s", l.cfg.HoldersRefreshInterval)
	}

	// Unset falls back to the documented defaults.
	def := NewLagCatchup(LagCatchupConfig{})
	if def.cfg.Timeout != LagCatchupTimeout {
		t.Errorf("default Timeout = %v, want %v", def.cfg.Timeout, LagCatchupTimeout)
	}
	if def.cfg.HoldersRefreshInterval != holdersRefreshInterval {
		t.Errorf("default HoldersRefreshInterval = %v, want %v",
			def.cfg.HoldersRefreshInterval, holdersRefreshInterval)
	}
}
