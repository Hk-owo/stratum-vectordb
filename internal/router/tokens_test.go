package router

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTokens(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestTokenTable_LoadsTheDocumentedShape pins the file format the deployment
// writes by hand.
func TestTokenTable_LoadsTheDocumentedShape(t *testing.T) {
	path := writeTokens(t, `
tokens:
  - token: "sk-demo-a-xxxxxxxx"
    kb_ids: ["kb-ragdemo-a", "kb-ragdemo-a-eval"]
  - token: "sk-demo-b-xxxxxxxx"
    kb_ids: ["kb-interview-prep"]
`)
	table, err := LoadTokenTable(path)
	if err != nil {
		t.Fatalf("LoadTokenTable: %v", err)
	}
	if table.Len() != 2 {
		t.Fatalf("loaded %d credentials, want 2", table.Len())
	}

	a, ok := table.Lookup("sk-demo-a-xxxxxxxx")
	if !ok {
		t.Fatal("the first credential did not resolve")
	}
	for _, kb := range []string{"kb-ragdemo-a", "kb-ragdemo-a-eval"} {
		if !a.Allows(kb, false) || !a.Allows(kb, true) {
			t.Errorf("token a should reach %s for both verbs", kb)
		}
	}
	if a.Allows("kb-interview-prep", false) {
		t.Error("token a must not reach a knowledge base outside its list")
	}
	if _, ok := table.Lookup("sk-not-issued"); ok {
		t.Error("an unknown token must not resolve")
	}
}

// TestTokenTable_RejectsMisconfiguration keeps a broken table from looking like
// a working one: a station that loaded nothing would deny every caller, which is
// indistinguishable from a table that loaded wrong.
func TestTokenTable_RejectsMisconfiguration(t *testing.T) {
	for name, body := range map[string]string{
		"empty list":  "tokens: []\n",
		"empty token": "tokens:\n  - token: \"\"\n    kb_ids: [\"kb-1\"]\n",
		"no kb_ids":   "tokens:\n  - token: \"sk-a\"\n    kb_ids: []\n",
		"duplicate":   "tokens:\n  - token: \"sk-a\"\n    kb_ids: [\"kb-1\"]\n  - token: \"sk-a\"\n    kb_ids: [\"kb-2\"]\n",
		"empty kb_id": "tokens:\n  - token: \"sk-a\"\n    kb_ids: [\"\"]\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadTokenTable(writeTokens(t, body)); err == nil {
				t.Error("expected the load to fail")
			}
		})
	}
}

// TestTokenTable_MissingFileIsFatal: no table is not "no credentials allowed",
// it is a misconfigured station, and the caller must be told.
func TestTokenTable_MissingFileIsFatal(t *testing.T) {
	if _, err := LoadTokenTable(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("expected a missing token table to fail the load")
	}
}
