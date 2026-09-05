package service

// quantizer_mapping_test.go — round-trips the KB quantizer mapping
// between the console-facing proto enum (QUANTIZER_*) and the short
// internal name stored in KnowledgeBaseMeta (Stratum_设计文档v12.md 2.4).

import (
	"testing"

	pb "stratum/api/proto/stratum"
)

func TestQuantizerMappingRoundTrip(t *testing.T) {
	cases := map[string]pb.QuantizerType{
		"OFF":     pb.QuantizerType_QUANTIZER_OFF,
		"SQ8":     pb.QuantizerType_QUANTIZER_SQ8,
		"SQ_BF16": pb.QuantizerType_QUANTIZER_SQ_BF16,
		"SQ_FP16": pb.QuantizerType_QUANTIZER_SQ_FP16,
		"PQ":      pb.QuantizerType_QUANTIZER_PQ,
	}
	for short, want := range cases {
		if got := quantizerToProto(short); got != want {
			t.Errorf("quantizerToProto(%q) = %v, want %v", short, got, want)
		}
		if back := quantizerFromProto(want); back != short {
			t.Errorf("quantizerFromProto(%v) = %q, want %q", want, back, short)
		}
	}
}

func TestQuantizerMappingDefaults(t *testing.T) {
	// Unknown / unset values fall back to OFF.
	if got := quantizerFromProto(pb.QuantizerType_QUANTIZER_OFF); got != "OFF" {
		t.Errorf("OFF → %q, want OFF", got)
	}
	if got := quantizerToProto(""); got != pb.QuantizerType_QUANTIZER_OFF {
		t.Errorf("unknown short name → %v, want OFF", got)
	}
}
