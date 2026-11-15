package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

// MigrationManifest freezes DATA requirements, not advisory index status. Failed
// DATA versions are retained as metadata but make no claim to complete bytes.
type MigrationVersion struct {
	ID           int64            `json:"id"`
	Parent       int64            `json:"parent"`
	DataStatus   types.DataStatus `json:"data_status"`
	DocIDSetHash string           `json:"doc_id_set_hash"`
}
type MigrationManifest struct {
	KBID          string             `json:"kb_id"`
	ActiveVersion int64              `json:"active_version"`
	Versions      []MigrationVersion `json:"versions"`
}

func ValidSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && s == hex.EncodeToString(b)
}
func BuildMigrationManifest(kb string, active int64, versions []types.VersionMeta) (MigrationManifest, error) {
	m := MigrationManifest{KBID: kb, ActiveVersion: active, Versions: make([]MigrationVersion, 0, len(versions))}
	if kb == "" || active < 0 {
		return m, serrors.ErrInvalidArgument
	}
	activePresent := active == 0
	for _, v := range versions {
		if v.KBID != kb || v.VersionID <= 0 || v.ParentVersionID < 0 || v.ParentVersionID >= v.VersionID {
			return m, serrors.ErrInvalidArgument
		}
		if v.Deleting || v.DataStatus == types.DataStatusPending {
			return m, serrors.ErrMigrationBusy
		}
		if v.DataStatus != types.DataStatusDurable && v.DataStatus != types.DataStatusFailedPermanent {
			return m, serrors.ErrMigrationConflict
		}
		if v.DataStatus == types.DataStatusDurable && !ValidSHA256(v.DocIDSetHash) {
			return m, serrors.ErrMigrationBusy
		}
		if v.VersionID == active {
			if v.DataStatus != types.DataStatusDurable {
				return m, serrors.ErrMigrationConflict
			}
			activePresent = true
		}
		m.Versions = append(m.Versions, MigrationVersion{ID: v.VersionID, Parent: v.ParentVersionID, DataStatus: v.DataStatus, DocIDSetHash: v.DocIDSetHash})
	}
	sort.Slice(m.Versions, func(i, j int) bool { return m.Versions[i].ID < m.Versions[j].ID })
	for i := 1; i < len(m.Versions); i++ {
		if m.Versions[i-1].ID == m.Versions[i].ID {
			return m, serrors.ErrInvalidArgument
		}
	}
	if !activePresent {
		return m, serrors.ErrMigrationConflict
	}
	return m, nil
}
func (m MigrationManifest) Bytes() []byte { b, _ := json.Marshal(m); return b }
func (m MigrationManifest) Hash() string {
	sum := sha256.Sum256(m.Bytes())
	return hex.EncodeToString(sum[:])
}

type MigrationStageReceipt struct {
	SourceID     int64  `json:"source_id"`
	TargetID     int64  `json:"target_id"`
	ManifestHash string `json:"manifest_hash"`
	PayloadHash  string `json:"payload_hash"`
	Entries      uint64 `json:"entries"`
	Bytes        uint64 `json:"bytes"`
}

// RecordMigrationStage records an isolated, durable artifact. It does NOT
// authorize serving, ordinary writes, cursor advancement or placement changes.
func (d *Directory) RecordMigrationStage(op string, receipt MigrationStageReceipt, manifest MigrationManifest) error {
	m, ok := d.Migrations[op]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase != MigrationFenced && m.Phase != MigrationStaged {
		return serrors.ErrMigrationConflict
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	p, err := d.Lookup(m.Request.KBID)
	if err != nil {
		return err
	}
	if !equalPlacement(p, m.Old) {
		return serrors.ErrPlacementStale
	}
	if manifest.KBID != m.Request.KBID || receipt.ManifestHash != manifest.Hash() || !ValidSHA256(receipt.PayloadHash) || receipt.Bytes == 0 || receipt.TargetID != m.Request.ToID || !m.Old.Contains(receipt.SourceID) || (m.Request.Fault && receipt.SourceID == m.Request.FromID) {
		return serrors.ErrMigrationConflict
	}
	for _, id := range m.RequiredFenceIDs() {
		if !containsNode(m.FencedIDs, id) {
			return serrors.ErrMigrationBusy
		}
	}
	if !containsNode(m.FencedIDs, receipt.SourceID) {
		return serrors.ErrMigrationConflict
	}
	if m.Stage != nil {
		if *m.Stage == receipt {
			return nil
		}
		return serrors.ErrMigrationConflict
	}
	m.Stage = &receipt
	m.Phase = MigrationStaged
	d.Migrations[op] = m
	d.Revision++
	return nil
}
func containsNode(ids []int64, id int64) bool {
	for _, n := range ids {
		if n == id {
			return true
		}
	}
	return false
}

// MigrationInstallReceipt is the target's proof that it wrote a staged artifact
// into its OWN ordinary stores and re-read the result. It carries no claim about
// membership, serving, ready indexes or future write admission: the effective
// replica set and epoch stay exactly as they were before installation.
type MigrationInstallReceipt struct {
	SourceID     int64  `json:"source_id"`
	TargetID     int64  `json:"target_id"`
	ManifestHash string `json:"manifest_hash"`
	StageHash    string `json:"stage_hash"`
	Versions     uint64 `json:"versions"`
	Documents    uint64 `json:"documents"`
	Chunks       uint64 `json:"chunks"`
}

// RecordMigrationInstall turns a STAGED task into INSTALLED. The receipt is
// checked against the frozen stage receipt and against the DATA manifest
// recomputed by the caller from committed metadata, so a target cannot report an
// installation of something else. Installation is repeatable but not replaceable:
// a different receipt for the same task is a conflict, never an upgrade.
func (d *Directory) RecordMigrationInstall(op string, receipt MigrationInstallReceipt, manifest MigrationManifest) error {
	m, ok := d.Migrations[op]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase != MigrationStaged && m.Phase != MigrationInstalled {
		return serrors.ErrMigrationConflict
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	if m.Stage == nil {
		return serrors.ErrMigrationConflict
	}
	p, err := d.Lookup(m.Request.KBID)
	if err != nil {
		return err
	}
	if !equalPlacement(p, m.Old) {
		return serrors.ErrPlacementStale
	}
	if manifest.KBID != m.Request.KBID || receipt.ManifestHash != manifest.Hash() || !ValidSHA256(receipt.ManifestHash) {
		return serrors.ErrMigrationConflict
	}
	if receipt.StageHash != m.Stage.PayloadHash || receipt.TargetID != m.Request.ToID || receipt.SourceID != m.Stage.SourceID {
		return serrors.ErrMigrationConflict
	}
	if want := durableVersionCount(manifest); receipt.Versions != want {
		return serrors.ErrMigrationConflict
	}
	if m.Install != nil {
		if *m.Install == receipt {
			return nil
		}
		return serrors.ErrMigrationConflict
	}
	m.Install = &receipt
	m.Phase = MigrationInstalled
	d.Migrations[op] = m
	d.Revision++
	return nil
}

// MigrationServeReceipt is the target's local serving evidence: its data cursor
// covers the manifest's active version, every DURABLE version's document filter
// was rebuilt from the installed data, and the active version's index artifact is
// durable on this node. It is deliberately a DIFFERENT record from the install
// receipt — index readiness never becomes part of the DATA proof, and neither
// record activates anything.
type MigrationServeReceipt struct {
	TargetID      int64  `json:"target_id"`
	ManifestHash  string `json:"manifest_hash"`
	ActiveVersion int64  `json:"active_version"`
	CursorVersion int64  `json:"cursor_version"`
	BloomVersions uint64 `json:"bloom_versions"`
	IndexReady    bool   `json:"index_ready"`
}

// RecordMigrationServe records that evidence for an INSTALLED task. The phase does
// not change: activation is a separate committed decision that must check this
// record, not be implied by it.
func (d *Directory) RecordMigrationServe(op string, receipt MigrationServeReceipt, manifest MigrationManifest) error {
	m, ok := d.Migrations[op]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase != MigrationInstalled {
		return serrors.ErrMigrationConflict
	}
	if m.Install == nil {
		return serrors.ErrMigrationConflict
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	p, err := d.Lookup(m.Request.KBID)
	if err != nil {
		return err
	}
	if !equalPlacement(p, m.Old) {
		return serrors.ErrPlacementStale
	}
	if manifest.KBID != m.Request.KBID || receipt.ManifestHash != manifest.Hash() || receipt.ManifestHash != m.Install.ManifestHash {
		return serrors.ErrMigrationConflict
	}
	if receipt.TargetID != m.Request.ToID || receipt.ActiveVersion != manifest.ActiveVersion {
		return serrors.ErrMigrationConflict
	}
	// A target that has not reached the active version could not answer a query
	// for it, so its cursor must cover the manifest — not merely the installation.
	if receipt.CursorVersion < manifest.ActiveVersion {
		return serrors.ErrMigrationBusy
	}
	if receipt.BloomVersions != durableVersionCount(manifest) {
		return serrors.ErrMigrationBusy
	}
	if !receipt.IndexReady {
		return serrors.ErrMigrationBusy
	}
	if m.Serve != nil {
		if *m.Serve == receipt {
			return nil
		}
		return serrors.ErrMigrationConflict
	}
	m.Serve = &receipt
	d.Migrations[op] = m
	d.Revision++
	return nil
}

func durableVersionCount(manifest MigrationManifest) uint64 {
	var n uint64
	for _, v := range manifest.Versions {
		if v.DataStatus == types.DataStatusDurable {
			n++
		}
	}
	return n
}

// MigrationTransferView is one quorum-confirmed control snapshot. Separate
// directory and version-list reads must never define migration requirements.
type MigrationTransferView struct {
	Directory Directory
	Manifest  MigrationManifest
}
type MigrationTransferReader interface {
	GetKBMigrationTransfer(context.Context, string) (MigrationTransferView, error)
}
