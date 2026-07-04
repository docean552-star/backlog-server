package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeYAML dumps content into tmpDir/name.yaml and returns the tmpDir.
// Individual tests write their own taskowners.yaml so we don't accidentally
// depend on the repo's live one.
func writeYAML(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return dir
}

func TestLoadTaskowners_MissingFileReturnsEmpty(t *testing.T) {
	dir := t.TempDir() // no taskowners.yaml here
	r, err := LoadTaskowners(dir)
	if err != nil {
		t.Fatalf("LoadTaskowners: %v", err)
	}
	if got := r.Rules(); len(got) != 0 {
		t.Fatalf("expected empty rules, got %d", len(got))
	}
}

func TestLoadTaskowners_SpecificitySortDesc(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
    owner: samvel
  - patterns:
      mode: Code
      owner: A
      tags: [ui]
    owner: A
  - patterns: {}
    owner: fallback
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, err := LoadTaskowners(dir)
	if err != nil {
		t.Fatalf("LoadTaskowners: %v", err)
	}
	rules := r.Rules()
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}
	if got := len(rules[0].Patterns); got != 3 {
		t.Errorf("rule[0] should have 3 patterns (most specific), got %d", got)
	}
	if got := len(rules[1].Patterns); got != 1 {
		t.Errorf("rule[1] should have 1 pattern, got %d", got)
	}
	if got := len(rules[2].Patterns); got != 0 {
		t.Errorf("rule[2] should be catch-all (0 patterns), got %d", got)
	}
}

func TestFieldMatches_AllSixPatternTypes(t *testing.T) {
	task := TaskFacts{
		Title:     "Fix Login Bug in Angular",
		Owner:     "A",
		Mode:      "Code",
		Status:    "READY",
		Tags:      []string{"ui", "bugfix"},
		Consumers: []string{"marketing", "seo"},
	}
	cases := []struct {
		name     string
		field    string
		expected any
		want     bool
	}{
		{"tags string hit", "tags", "ui", true},
		{"tags string miss", "tags", "backend", false},
		{"tags list hit", "tags", []any{"backend", "ui"}, true},
		{"tags list miss", "tags", []any{"backend", "infra"}, false},
		{"consumers hit", "consumers", "seo", true},
		{"consumers list hit", "consumers", []any{"smm", "seo"}, true},
		{"consumers miss", "consumers", "unknown", false},
		{"mode case-insensitive", "mode", "code", true},
		{"mode mismatch", "mode", "Think", false},
		{"title_contains hit", "title_contains", "login", true},
		{"title_contains case-insensitive", "title_contains", "ANGULAR", true},
		{"title_contains miss", "title_contains", "database", false},
		{"owner hit case-insensitive", "owner", "a", true},
		{"owner miss", "owner", "B", false},
		{"status hit", "status", "READY", true},
		{"status miss", "status", "DONE", false},
		{"unknown field is not a match", "priority", "high", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := fieldMatches(task, c.field, c.expected); got != c.want {
				t.Errorf("%s: got %v want %v", c.name, got, c.want)
			}
		})
	}
}

func TestMatchRule_SingleMatch(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
      owner: samvel
    owner: samvel
    workflow: code_task
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Code", Owner: "samvel"}
	rule := r.MatchRule(facts, nil)
	if rule == nil {
		t.Fatal("expected match, got nil")
	}
	if rule.Owner != "samvel" || rule.Workflow != "code_task" {
		t.Errorf("wrong rule: %+v", rule)
	}
}

func TestMatchRule_EmptyMatch(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
      owner: samvel
    owner: samvel
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Think", Owner: "A"} // no match
	if rule := r.MatchRule(facts, nil); rule != nil {
		t.Errorf("expected nil, got %+v", rule)
	}
}

func TestMatchRule_MultiMatch_SpecificityWins(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
    owner: catchall
  - patterns:
      mode: Code
      owner: samvel
      tags: [go]
    owner: samvel
    workflow: code_task
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Code", Owner: "samvel", Tags: []string{"go"}}
	rule := r.MatchRule(facts, nil)
	if rule == nil || rule.Owner != "samvel" {
		t.Fatalf("expected samvel (more specific), got %+v", rule)
	}
}

func TestMatchRule_PriorityBreaksSpecificityTie(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
    owner: low
    priority: 1
  - patterns:
      mode: Code
    owner: high
    priority: 10
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Code"}
	rule := r.MatchRule(facts, nil)
	if rule == nil || rule.Owner != "high" {
		t.Fatalf("expected high (higher priority), got %+v", rule)
	}
}

func TestMatchRule_CapacityBreaksPriorityTie(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
    owner: busy
    priority: 5
  - patterns:
      mode: Code
    owner: free
    priority: 5
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Code"}
	// Busy owner has 10 IN_PROGRESS, free has 0.
	cap := func(owner string) int {
		if owner == "busy" {
			return 10
		}
		return 0
	}
	rule := r.MatchRule(facts, cap)
	if rule == nil || rule.Owner != "free" {
		t.Fatalf("expected free (lower capacity), got %+v", rule)
	}
}

func TestSuggestMatches_LimitAndDedupe(t *testing.T) {
	yaml := `rules:
  - patterns:
      mode: Code
    owner: samvel
  - patterns:
      mode: Code
    owner: samvel
  - patterns:
      mode: Code
    owner: A
  - patterns:
      mode: Code
    owner: B
`
	dir := writeYAML(t, "taskowners.yaml", yaml)
	r, _ := LoadTaskowners(dir)
	facts := TaskFacts{Mode: "Code"}
	got := r.SuggestMatches(facts, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 suggestions, got %d: %v", len(got), got)
	}
	// samvel dedup'd, A and B still both distinct.
	if got[0] != "samvel" {
		t.Errorf("first suggestion should be samvel, got %s", got[0])
	}
}
