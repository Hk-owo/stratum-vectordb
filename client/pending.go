// Package client is a minimal Stratum client: the caller's side of
// docs/client-integration-guide.md, written down as code.
//
// It is deliberately NOT an SDK — no retry policy, no connection pool, no
// framework. What it provides is the part every caller has to get right on its
// own (docs/await-version-plan.md §12 item 4): a durable local record of each
// in-flight batch. The cluster holds no changes for anyone (the only persistent
// copy outside this process appears once the coordinator writes its WAL BEGIN),
// so that record is what turns "re-send or give up" from a slogan into a
// decision a program can actually make.
package client

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	pb "stratum/api/proto/stratum"
)

// Change is one document change, in the shape the wire uses.
type Change struct {
	Op      string `json:"op"` // ADD | UPDATE | DELETE
	DocID   string `json:"doc_id"`
	Content string `json:"content,omitempty"`
}

// ToProto converts the change for the wire. An unrecognized op is an error
// rather than a silent ADD: the caller wrote something it did not mean, and
// guessing would turn a typo into a document.
func (c Change) ToProto() (*pb.DocChange, error) {
	out := &pb.DocChange{DocId: c.DocID, Content: c.Content}
	switch c.Op {
	case "ADD", "add":
		out.Op = pb.ChangeOp_CHANGE_OP_ADD
	case "UPDATE", "update":
		out.Op = pb.ChangeOp_CHANGE_OP_UPDATE
	case "DELETE", "delete":
		out.Op = pb.ChangeOp_CHANGE_OP_DELETE
	default:
		return nil, fmt.Errorf("client: unknown change op %q (want ADD, UPDATE or DELETE)", c.Op)
	}
	if out.Op != pb.ChangeOp_CHANGE_OP_DELETE && c.Content == "" {
		return nil, fmt.Errorf("client: change %s/%s has no content (ADD and UPDATE need the whole document)", c.Op, c.DocID)
	}
	return out, nil
}

// PendingWrite is one submitted batch, as this process remembers it.
//
// Changes travel with the record because they are the caller's only copy until
// the coordinator's WAL BEGIN lands, and ClientRequestID travels with it because
// a re-send under the SAME key is what makes the retry idempotent — the two
// together are what let a caller recover a write whose data never landed.
type PendingWrite struct {
	ID              string    `json:"id"`
	KnowledgeBaseID string    `json:"knowledge_base_id"`
	VersionID       int64     `json:"version_id"`
	ClientRequestID string    `json:"client_request_id"`
	Changes         []Change  `json:"changes"`
	SubmittedAt     time.Time `json:"submitted_at"`
	Settled         bool      `json:"settled"`
}

// Store keeps pending writes in one JSON file.
//
// One file, rewritten atomically (temp + rename), because the failure this
// exists for is a crash: a half-written state file would lose exactly the
// records that matter. It is not a database and does not pretend to be — a
// caller with many concurrent batches should write its own.
type Store struct {
	path string
}

// NewStore returns a store backed by <dir>/pending.json, creating the directory.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("client: create state dir %s: %w", dir, err)
	}
	return &Store{path: filepath.Join(dir, "pending.json")}, nil
}

// Path is where the records live, for logs and for an operator looking for them.
func (s *Store) Path() string { return s.path }

// Load returns every record, newest first. A missing file is an empty store, not
// an error: the first run has nothing to remember yet.
func (s *Store) Load() ([]PendingWrite, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("client: read %s: %w", s.path, err)
	}
	var out []PendingWrite
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("client: parse %s: %w", s.path, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SubmittedAt.After(out[j].SubmittedAt) })
	return out, nil
}

func (s *Store) saveAll(records []PendingWrite) error {
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("client: encode state: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("client: write %s: %w", tmp, err)
	}
	// Rename last: the state file is either the previous version or the new one,
	// never a mixture.
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("client: replace %s: %w", s.path, err)
	}
	return nil
}

// Upsert records a write (or replaces it, keyed by ID).
func (s *Store) Upsert(w PendingWrite) error {
	records, err := s.Load()
	if err != nil {
		return err
	}
	replaced := false
	for i := range records {
		if records[i].ID == w.ID {
			records[i] = w
			replaced = true
			break
		}
	}
	if !replaced {
		records = append(records, w)
	}
	return s.saveAll(records)
}

// Get returns one record by ID.
func (s *Store) Get(id string) (PendingWrite, bool, error) {
	records, err := s.Load()
	if err != nil {
		return PendingWrite{}, false, err
	}
	for _, w := range records {
		if w.ID == id {
			return w, true, nil
		}
	}
	return PendingWrite{}, false, nil
}

// Decision is what a caller should do about a version it is waiting on.
type Decision int

const (
	// DecisionWait: ask again after the server's retry interval.
	DecisionWait Decision = iota
	// DecisionDone: the write landed.
	DecisionDone
	// DecisionResend: submit the same changes under the same idempotency key.
	DecisionResend
	// DecisionDiscard: the version will never land and the changes are gone.
	DecisionDiscard
)

func (d Decision) String() string {
	switch d {
	case DecisionDone:
		return "DONE"
	case DecisionResend:
		return "RESEND"
	case DecisionDiscard:
		return "DISCARD"
	default:
		return "WAIT"
	}
}

// The stages AwaitVersion reports. They mirror service.Stage* — kept as literals
// here so this package does not link the server's implementation, and pinned by
// TestStagesMatchTheServiceConstants.
const (
	stageDataPending          = "DATA_PENDING"
	stageDataDurable          = "DATA_DURABLE"
	stageIndexReady           = "INDEX_READY"
	stageIndexFailed          = "INDEX_FAILED"
	stageDataFailedPermanent  = "DATA_FAILED_PERMANENT"
	stageIndexFailedPermanent = "INDEX_FAILED_PERMANENT"
	stageDeleting             = "DELETING"
)

// Decide turns one AwaitVersion answer plus the local record into the caller's
// next move.
//
// The `haveChanges` argument is the whole reason this function exists rather
// than being part of the server's answer: the cluster can prove "no reachable
// replica holds this data" (data_missing), but whether the changes still exist
// anywhere is a fact only the caller has. That is why the server reports the
// fact and the caller decides, and why a caller that has thrown its changes away
// can only discard (docs/client-integration-guide.md §7).
func Decide(resp *pb.AwaitVersionResponse, haveChanges bool) Decision {
	switch resp.GetStage() {
	case stageIndexReady:
		return DecisionDone

	case stageDataFailedPermanent, stageIndexFailedPermanent:
		// A settled verdict on either side. The version is retired, so the same
		// key would only hand back a retired version number (§7.12): starting
		// over means a NEW key, which is the caller's business, not ours.
		if haveChanges {
			return DecisionResend
		}
		return DecisionDiscard

	case stageIndexFailed:
		// Rebuildable, and not the caller's data problem: waiting is what the
		// plan's flow prescribes (AdminService.RebuildIndex, then ask again).
		return DecisionWait

	case stageDeleting:
		return DecisionDiscard

	case stageDataPending:
		// "Still writing" and "will never write" are the same stage; only the
		// existence probe separates them.
		if resp.GetDataMissing() {
			if haveChanges {
				return DecisionResend
			}
			return DecisionDiscard
		}
		return DecisionWait

	case stageDataDurable:
		// Durable but not queryable yet: the caller asked for READY, so keep
		// waiting. (A caller that only wants durability would have asked for it.)
		return DecisionWait

	default:
		// An unknown stage is a newer server's answer. Waiting is the only safe
		// reading: it never destroys anything.
		return DecisionWait
	}
}

// newRequestID mints this client's idempotency keys: time first so a human can
// order them in a log, plus a random suffix so two processes starting in the same
// nanosecond do not collide. Uniqueness here is not cosmetic — a collision would
// make two different batches collapse into one version.
func newRequestID() string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		// crypto/rand failing is not a condition to paper over with a weaker key.
		panic(fmt.Sprintf("client: no randomness for an idempotency key: %v", err))
	}
	return fmt.Sprintf("cli-%d-%s", time.Now().UnixNano(), hex.EncodeToString(suffix[:]))
}
