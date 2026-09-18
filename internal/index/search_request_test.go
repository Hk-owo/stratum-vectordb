package index

// search_request_test.go — the coarse-pass budget (§2.2) has to reach the wire.
//
// SearchIndexRequest.candidate_n was implemented on both ends long before it had
// a caller: the proto declared it, the vector store honoured it (falling back to
// clamp(top_k × 8, 16, 4096) at 0), and the Go side never set it — so the only
// way to change the candidate budget was to edit a C++ constant and rebuild.
// These two cases pin the wire behaviour that closes that gap.

import "testing"

func TestSearchRequest_CarriesTheConfiguredCandidateBudget(t *testing.T) {
	im := &IndexManagerImpl{cfg: IndexManagerConfig{CandidateN: 128}}

	req := im.searchRequest("kb-1", 7, []float32{1, 2, 3}, 5)

	if req.CandidateN != 128 {
		t.Errorf("CandidateN = %d, want 128", req.CandidateN)
	}
	if req.KbId != "kb-1" || req.VersionId != 7 || req.TopK != 5 {
		t.Errorf("request lost its identity: %+v", req)
	}
	if len(req.Vector) != 3 {
		t.Errorf("Vector = %v, want the query vector", req.Vector)
	}
}

// Unset has to mean "leave it to the vector store": it already reads 0 as "use
// my default", and a node that does not configure the knob should stay
// byte-identical on the wire to one built before the knob existed.
func TestSearchRequest_UnsetCandidateBudgetLeavesTheFieldAlone(t *testing.T) {
	im := &IndexManagerImpl{cfg: IndexManagerConfig{}}

	if got := im.searchRequest("kb-1", 7, []float32{1}, 5).CandidateN; got != 0 {
		t.Errorf("CandidateN = %d, want 0 (unset → vector store default)", got)
	}
}
