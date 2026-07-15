package store

import (
	"reflect"
	"testing"
)

func TestComputeAdvanceTarget(t *testing.T) {
	cases := []struct {
		name     string
		current  string
		workflow string
		want     string
	}{
		// code_task lifecycle (existing behavior preserved)
		{"code_task BACKLOG", "BACKLOG", "code_task", "PLANNING"},
		{"code_task PLANNING", "PLANNING", "code_task", "READY"},
		{"code_task READY", "READY", "code_task", "IN_PROGRESS"},
		{"code_task IN_PROGRESS", "IN_PROGRESS", "code_task", "IN_REVIEW"},
		{"code_task IN_REVIEW", "IN_REVIEW", "code_task", "AWAITING_APPROVAL"},
		{"code_task AWAITING_APPROVAL", "AWAITING_APPROVAL", "code_task", "DONE"},
		{"empty workflow treated as code_task", "IN_PROGRESS", "", "IN_REVIEW"},
		{"workflow whitespace trimmed", "IN_PROGRESS", "  code_task  ", "IN_REVIEW"},
		{"workflow case-insensitive", "IN_PROGRESS", "CODE_TASK", "IN_REVIEW"},

		// fix workflow (#1619)
		{"fix BACKLOG", "BACKLOG", "fix", "SCOPED"},
		{"fix SCOPED", "SCOPED", "fix", "IN_PROGRESS"},
		{"fix IN_PROGRESS", "IN_PROGRESS", "fix", "IN_REVIEW"},
		{"fix IN_REVIEW", "IN_REVIEW", "fix", "AWAITING_APPROVAL"},
		{"fix AWAITING_APPROVAL", "AWAITING_APPROVAL", "fix", "DONE"},
		// fix does NOT have PLANNING / READY
		{"fix has no PLANNING", "PLANNING", "fix", ""},
		{"fix has no READY", "READY", "fix", ""},

		// think_task workflow (#1619)
		{"think_task BACKLOG", "BACKLOG", "think_task", "PLANNING"},
		{"think_task PLANNING", "PLANNING", "think_task", "READY"},
		{"think_task READY", "READY", "think_task", "IN_PROGRESS"},
		{"think_task IN_PROGRESS", "IN_PROGRESS", "think_task", "REVIEW"},
		{"think_task REVIEW", "REVIEW", "think_task", "AWAITING_APPROVAL"},
		{"think_task AWAITING_APPROVAL", "AWAITING_APPROVAL", "think_task", "DONE"},
		// think_task uses REVIEW not IN_REVIEW
		{"think_task has no IN_REVIEW", "IN_REVIEW", "think_task", ""},

		// unknown workflows fall through to /exec (server returns "")
		{"research_task not native", "IN_PROGRESS", "research_task", ""},
		{"marketing not native", "IN_PROGRESS", "marketing", ""},
		{"seo not native", "IN_PROGRESS", "seo", ""},
		{"smm_task not native", "IN_PROGRESS", "smm_task", ""},

		// unknown status inside native workflow → ""
		{"unknown status code_task", "MITIGATED", "code_task", ""},
		{"unknown status fix", "REVIEW", "fix", ""}, // fix has IN_REVIEW, not REVIEW
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeAdvanceTarget(tc.current, tc.workflow)
			if got != tc.want {
				t.Errorf("ComputeAdvanceTarget(%q, %q) = %q; want %q",
					tc.current, tc.workflow, got, tc.want)
			}
		})
	}
}

func TestDefaultReviewers(t *testing.T) {
	cases := []struct {
		name     string
		workflow string
		mode     string
		want     []string
	}{
		// Workflow branch — precedence over mode
		{"think_task workflow", "think_task", "code", []string{"content-reviewer", "task-closure-reviewer"}},
		{"think_task workflow ignores mode", "think_task", "Think", []string{"content-reviewer", "task-closure-reviewer"}},
		{"fix workflow", "fix", "code", []string{"code-reviewer", "task-closure-reviewer"}},
		{"code_task workflow", "code_task", "code", []string{"code-reviewer", "task-closure-reviewer"}},
		{"workflow case-insensitive", "THINK_TASK", "", []string{"content-reviewer", "task-closure-reviewer"}},

		// Legacy: empty workflow falls through to mode
		{"empty workflow, code mode", "", "code", []string{"code-reviewer", "task-closure-reviewer"}},
		{"empty workflow, Code mode", "", "Code", []string{"code-reviewer", "task-closure-reviewer"}},
		{"empty workflow, think mode", "", "think", []string{"content-reviewer", "task-closure-reviewer"}},
		{"empty workflow, unknown mode", "", "seo", []string{"task-closure-reviewer"}},
		{"empty workflow, empty mode", "", "", []string{"task-closure-reviewer"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultReviewers(tc.workflow, tc.mode)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("defaultReviewers(%q, %q) = %v; want %v",
					tc.workflow, tc.mode, got, tc.want)
			}
		})
	}
}
