package wire

import (
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// VersionFromInfo must carry every field VersionInfo actually has. A field left at
// its zero value does not read as "absent" downstream — it reads as a fact, and two
// of them point the unsafe way:
//
//   - PENDING for a version whose data is durable, which is what made every
//     restarting storage node report cursor 0 (measured in the cluster);
//   - "not deleting" for a version being reclaimed, which is what lets a node keep
//     serving data the control layer has already retired.
func TestVersionFromInfo_CarriesEveryFieldTheProtoHas(t *testing.T) {
	got := VersionFromInfo("kb-1", &pb.VersionInfo{
		VersionId:       7,
		ParentVersionId: 6,
		CreatedAt:       1234,
		IndexStatus:     pb.IndexStatus_INDEX_STATUS_READY,
		DataStatus:      pb.DataStatus_DATA_STATUS_DURABLE,
		Deleting:        true,
		DocIdSetHash:    "digest-of-the-document-set",
		IndexReadyNodes: []int64{11, 13},
	})

	if got.VersionID != 7 || got.ParentVersionID != 6 || got.KBID != "kb-1" || got.CreatedAt != 1234 {
		t.Errorf("identity fields = %+v", got)
	}
	if got.IndexStatus != types.IndexStatusReady {
		t.Errorf("index status = %v, want READY", got.IndexStatus)
	}
	if got.DataStatus != types.DataStatusDurable {
		t.Errorf("data status = %v, want DATA_DURABLE — this is the field whose absence "+
			"made a restarting replica report cursor 0", got.DataStatus)
	}
	if !got.Deleting {
		t.Error("deleting = false, want true: a dropped flag reads as a positive fact downstream")
	}
	// DocIDSetHash has to make the trip whole: it is what separates "this version
	// has no documents" from "no digest was ever committed", and the storage layer
	// is the side that has to tell those apart.
	if got.DocIDSetHash != "digest-of-the-document-set" {
		t.Errorf("doc id set hash = %q, want the one the proto carried", got.DocIDSetHash)
	}
	// IndexReadyNodes is the list §8.6(d)'s rolling cleanup counts before a
	// storage node takes itself out of service. Only the control layer aggregates
	// it and only this route carries it to a storage node, so dropping it here
	// zeroed that count and made collection impossible — measured as
	// `others_serving=0 minimum_required=2` on a version three replicas served.
	if len(got.IndexReadyNodes) != 2 || got.IndexReadyNodes[0] != 11 || got.IndexReadyNodes[1] != 13 {
		t.Errorf("index ready nodes = %v, want [11 13]", got.IndexReadyNodes)
	}
}

// The data-side conversion is conservative in the direction that matters: anything
// unrecognised is PENDING, never DURABLE.
func TestDataStatusFromProto_DefaultsToPending(t *testing.T) {
	cases := []struct {
		in   pb.DataStatus
		want types.DataStatus
	}{
		{pb.DataStatus_DATA_STATUS_DURABLE, types.DataStatusDurable},
		{pb.DataStatus_DATA_STATUS_FAILED_PERMANENT, types.DataStatusFailedPermanent},
		{pb.DataStatus_DATA_STATUS_PENDING, types.DataStatusPending},
		{pb.DataStatus(99), types.DataStatusPending},
	}
	for _, c := range cases {
		if got := dataStatusFromProto(c.in); got != c.want {
			t.Errorf("dataStatusFromProto(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
