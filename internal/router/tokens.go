package router

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TokenTable is the service station's credential table, loaded from a static
// YAML file at startup (Stratum_设计文档v13.md §9.3(5)).
//
// Static, loaded once, and deliberately not watched. Adding or removing a demo
// credential is rarer than restarting the station; file watching and hot reload
// exist for credentials that change often while a service must not stop, and
// paying that complexity here would be paying it for nothing.
type TokenTable struct {
	principals map[string]Principal
}

// tokenFile mirrors the on-disk schema:
//
//	tokens:
//	  - token: "sk-demo-a-xxxxxxxx"
//	    kb_ids: ["kb-ragdemo-a", "kb-ragdemo-a-eval"]
//	  - token: "sk-demo-b-xxxxxxxx"
//	    kb_ids: ["kb-interview-prep"]
type tokenFile struct {
	Tokens []struct {
		Token string   `yaml:"token"`
		KBIDs []string `yaml:"kb_ids"`
	} `yaml:"tokens"`
}

// LoadTokenTable reads the credential table from path.
//
// A token's kb_ids list is the whole of its authority: the granularity stops at
// the knowledge base because a KB is the only isolation unit the system has, and
// every KB in the list is the token's own — there is no second axis left to
// split. That is why an entry grants read and write together rather than
// carrying a verb: the verb would be a distinction with nothing on the other
// side of it.
//
// Misconfiguration fails the load rather than silently producing a table that
// authorizes nothing or authorizes twice over: an empty list, an empty token and
// a duplicate are all rejected with the file named, because a station that
// started with a broken table would look exactly like one with a working table
// that denies everything.
func LoadTokenTable(path string) (*TokenTable, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("router: read token table %s: %w", path, err)
	}
	var tf tokenFile
	if err := yaml.Unmarshal(raw, &tf); err != nil {
		return nil, fmt.Errorf("router: parse token table %s: %w", path, err)
	}
	if len(tf.Tokens) == 0 {
		return nil, fmt.Errorf("router: token table %s lists no tokens", path)
	}

	principals := make(map[string]Principal, len(tf.Tokens))
	for i, e := range tf.Tokens {
		if e.Token == "" {
			return nil, fmt.Errorf("router: token table %s: entry %d has an empty token", path, i)
		}
		if _, dup := principals[e.Token]; dup {
			return nil, fmt.Errorf("router: token table %s: entry %d repeats a token", path, i)
		}
		if len(e.KBIDs) == 0 {
			return nil, fmt.Errorf("router: token table %s: entry %d lists no kb_ids", path, i)
		}
		grants := make(map[string]Grant, len(e.KBIDs))
		for _, kb := range e.KBIDs {
			if kb == "" {
				return nil, fmt.Errorf("router: token table %s: entry %d has an empty kb_id", path, i)
			}
			grants[kb] = Grant{Read: true, Write: true}
		}
		// No tenant id: the table's unit is the credential, and inventorying
		// "who" is not something the routing layer needs in order to decide.
		principals[e.Token] = Principal{Grants: grants}
	}
	return &TokenTable{principals: principals}, nil
}

// Lookup resolves a token, satisfying the Authenticator's lookup shape.
func (t *TokenTable) Lookup(token string) (Principal, bool) {
	if t == nil {
		return Principal{}, false
	}
	p, ok := t.principals[token]
	return p, ok
}

// Authenticator returns an Authenticator over this table.
func (t *TokenTable) Authenticator() *Authenticator {
	return NewAuthenticator(t.Lookup)
}

// Len reports how many credentials the table holds — for startup logging, so an
// operator can see at a glance that the table loaded what they expected.
func (t *TokenTable) Len() int {
	if t == nil {
		return 0
	}
	return len(t.principals)
}
