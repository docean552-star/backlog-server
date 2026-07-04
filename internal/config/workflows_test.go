package config

import (
	"testing"
)

func TestLoadWorkflows_MissingFileFallsBackToCodeTask(t *testing.T) {
	dir := t.TempDir()
	r, err := LoadWorkflows(dir)
	if err != nil {
		t.Fatalf("LoadWorkflows: %v", err)
	}
	wf, ok := r.Get("code_task")
	if !ok {
		t.Fatal("code_task fallback missing")
	}
	if len(wf.Stages) == 0 || wf.Stages[0] != "BACKLOG" || wf.Stages[len(wf.Stages)-1] != "DONE" {
		t.Errorf("fallback code_task must start BACKLOG end DONE, got %v", wf.Stages)
	}
}

func TestGet_UnknownWorkflowReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	r, _ := LoadWorkflows(dir)
	if _, ok := r.Get("nonexistent_workflow"); ok {
		t.Error("expected false for unknown workflow name")
	}
}

func TestDeriveFromOwnerMode_MatchedRuleWins(t *testing.T) {
	dir := t.TempDir()
	r, _ := LoadWorkflows(dir)
	rule := &OwnerRule{Owner: "samvel", Workflow: "marketing"}
	got := r.DeriveFromOwnerMode("Code", rule)
	if got != "marketing" {
		t.Errorf("expected marketing (from rule), got %s", got)
	}
}

func TestDeriveFromOwnerMode_NilRuleFallsBackToMode(t *testing.T) {
	dir := t.TempDir()
	r, _ := LoadWorkflows(dir)
	cases := []struct {
		mode string
		want string
	}{
		{"Code", "code_task"},
		{"code", "code_task"},
		{"Think", "think_task"},
		{"THINK", "think_task"},
		{"Analytics", "code_task"}, // unknown modes default to code_task
		{"", "code_task"},
	}
	for _, c := range cases {
		if got := r.DeriveFromOwnerMode(c.mode, nil); got != c.want {
			t.Errorf("mode=%q: got %s want %s", c.mode, got, c.want)
		}
	}
}

func TestValidateCustom_HappyPath(t *testing.T) {
	cw := &CustomWorkflow{
		Stages: []string{"BACKLOG", "REVIEW", "DONE"},
		Gates: map[string]any{
			"REVIEW": map[string]any{"review": "code-reviewer", "verdict": "PASS"},
		},
	}
	if got := ValidateCustom(cw); len(got) != 0 {
		t.Errorf("expected 0 failures, got %d: %+v", len(got), got)
	}
}

func TestValidateCustom_NilRejected(t *testing.T) {
	got := ValidateCustom(nil)
	if len(got) != 1 || got[0].Check != "INVALID_CUSTOM_WORKFLOW" {
		t.Errorf("expected single INVALID_CUSTOM_WORKFLOW failure, got %+v", got)
	}
}

func TestValidateCustom_EmptyStagesRejected(t *testing.T) {
	got := ValidateCustom(&CustomWorkflow{Stages: []string{}})
	if len(got) != 1 {
		t.Errorf("expected 1 failure, got %d: %+v", len(got), got)
	}
}

func TestValidateCustom_FirstMustBeBacklog(t *testing.T) {
	cw := &CustomWorkflow{Stages: []string{"READY", "DONE"}}
	got := ValidateCustom(cw)
	found := false
	for _, f := range got {
		if f.Detail != "" && contains(f.Detail, "first stage must be BACKLOG") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected BACKLOG-first violation, got %+v", got)
	}
}

func TestValidateCustom_LastMustBeDone(t *testing.T) {
	cw := &CustomWorkflow{Stages: []string{"BACKLOG", "READY"}}
	got := ValidateCustom(cw)
	found := false
	for _, f := range got {
		if contains(f.Detail, "last stage must be DONE") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected DONE-last violation, got %+v", got)
	}
}

func TestValidateCustom_UnknownStageRejected(t *testing.T) {
	cw := &CustomWorkflow{Stages: []string{"BACKLOG", "TOTALLY_FAKE", "DONE"}}
	got := ValidateCustom(cw)
	found := false
	for _, f := range got {
		if contains(f.Detail, "not a known TaskStatus") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected unknown-stage violation, got %+v", got)
	}
}

func TestValidateCustom_GateKeyMustBeInStages(t *testing.T) {
	cw := &CustomWorkflow{
		Stages: []string{"BACKLOG", "REVIEW", "DONE"},
		Gates: map[string]any{
			"MISSING": map[string]any{"review": "code-reviewer"},
		},
	}
	got := ValidateCustom(cw)
	found := false
	for _, f := range got {
		if contains(f.Detail, "not in stages") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected gate-not-in-stages violation, got %+v", got)
	}
}

// contains is a tiny helper — internal/config is a fresh package so we
// import strings only when needed. This is one-line, keeps the test file
// stdlib-only.
func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
