package config

// Workflow registry — Go port of ax/backlogist/core/workflows.py's
// load-side. Parses workflows.yaml, exposes named workflows for owner+mode
// derivation, and validates inline custom workflows submitted via
// POST /tasks (`workflow:"custom"` + stages + gates).
//
// MVP surface (brief.md #1414 Q2 + Q5): named-workflow lookup (Get), owner+mode
// derivation (DeriveFromOwnerMode), custom-workflow schema validation
// (ValidateCustom). The `mixins:`/YAML-alias expansion, dict-style stage
// entries with per-stage `gates:` blocks, and TRANSITIONS mutation are
// deferred — this task only needs the stage sequence for derivation and the
// custom-workflow validator.
//
// Cache pattern mirrors TaskownersRegistry: LoadWorkflows once at startup,
// Reload does git-pull + mtime + reparse under single-flight.

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Workflow is the minimal set of fields we need for derivation + validation.
// Additional fields (planning_agents, review_agents, planning_context, etc.)
// live in the raw YAML but are not read server-side yet.
type Workflow struct {
	Name          string
	Stages        []string // uppercase, in order — plain string entries only for MVP
	ReviewAgents  []string
	PlanningAgents []string
}

// yamlWorkflow is the on-disk shape. `Stages` is []any because entries can
// be a plain string or a `{STAGE: {gates: [...]}}` dict — we accept both
// and extract the stage name, ignoring the gate block for MVP.
type yamlWorkflow struct {
	Stages          []any    `yaml:"stages"`
	PlanningAgents  []string `yaml:"planning_agents"`
	ReviewAgents    []string `yaml:"review_agents"`
	CodeReviewer    string   `yaml:"code_reviewer_model"`
	ReviewerProfile string   `yaml:"reviewer_profile"`
}

// yamlWorkflowsDoc = full document. `Workflows` is `{name: yamlWorkflow}`.
type yamlWorkflowsDoc struct {
	Workflows map[string]yamlWorkflow `yaml:"workflows"`
	// Mixins is present in the file but MVP-unused — kept in the schema so
	// yaml.Unmarshal doesn't error on the key.
	Mixins map[string]any `yaml:"mixins"`
}

// WorkflowRegistry holds a named lookup + reload machinery.
type WorkflowRegistry struct {
	dir       string
	workflows map[string]Workflow
	mtime     time.Time
	lastPull  time.Time // debounce (same rationale as TaskownersRegistry)
	mu        sync.RWMutex
	reloadMu  sync.Mutex
}

// LoadWorkflows parses workflows.yaml at dir/workflows.yaml once and returns
// the registry. Missing file → falls back to the hardcoded "code_task"
// default (mirrors Python _FALLBACK_WORKFLOW). Bad YAML → error.
func LoadWorkflows(dir string) (*WorkflowRegistry, error) {
	r := &WorkflowRegistry{dir: dir}
	if err := r.reparseLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *WorkflowRegistry) path() string {
	return filepath.Join(r.dir, "workflows.yaml")
}

// fallbackWorkflows returns the hardcoded code_task chain that used to be the
// static default before workflows.yaml existed (Python workflows.py:26-38).
// Used when the file is absent or empty.
func fallbackWorkflows() map[string]Workflow {
	return map[string]Workflow{
		"code_task": {
			Name: "code_task",
			Stages: []string{
				"BACKLOG", "PLANNING", "READY", "IN_PROGRESS",
				"IN_REVIEW", "AWAITING_APPROVAL", "DONE",
			},
			ReviewAgents:   []string{"code-reviewer"},
			PlanningAgents: []string{"domain-researcher", "spec-reviewer"},
		},
	}
}

// reparseLocked reads workflows.yaml, extracts stage-name sequences, and
// installs them under the write lock. Caller MUST hold reloadMu.
func (r *WorkflowRegistry) reparseLocked() error {
	p := r.path()
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			r.mu.Lock()
			r.workflows = fallbackWorkflows()
			r.mtime = time.Time{}
			r.mu.Unlock()
			return nil
		}
		return fmt.Errorf("stat %s: %w", p, err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("read %s: %w", p, err)
	}
	var doc yamlWorkflowsDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", p, err)
	}
	out := make(map[string]Workflow, len(doc.Workflows))
	for name, raw := range doc.Workflows {
		stages := extractStageNames(raw.Stages)
		out[name] = Workflow{
			Name:           name,
			Stages:         stages,
			ReviewAgents:   raw.ReviewAgents,
			PlanningAgents: raw.PlanningAgents,
		}
	}
	// Always keep code_task as a fallback entry even when the file omits it
	// (safety net for a partially-written workflows.yaml).
	if _, ok := out["code_task"]; !ok {
		fb := fallbackWorkflows()
		out["code_task"] = fb["code_task"]
	}
	r.mu.Lock()
	r.workflows = out
	r.mtime = fi.ModTime()
	r.mu.Unlock()
	return nil
}

// extractStageNames handles both plain strings and single-key dicts (with
// or without a nested `gates:` block). Mixins are ignored for MVP — the
// yaml.v3 unmarshaller already expands YAML `&`/`*` aliases before we see
// them, so a live workflows.yaml which uses only anchor aliases parses
// correctly; textual `"*name"` references are dropped (matches Python's
// undefined-mixin behavior at workflows.py:60-74).
func extractStageNames(raw []any) []string {
	if raw == nil {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		if s, ok := entry.(string); ok {
			out = append(out, upper(s))
			continue
		}
		if m, ok := entry.(map[string]any); ok {
			for k := range m {
				out = append(out, upper(k))
				break // single-key dict — take the first (only) key
			}
			continue
		}
	}
	return out
}

func upper(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		b[i] = c
	}
	return string(b)
}

// Reload: best-effort git pull + mtime + reparse. Same pattern as
// TaskownersRegistry.Reload.
func (r *WorkflowRegistry) Reload(ctx context.Context) error {
	// Debounce fast-path — see TaskownersRegistry.Reload for the full
	// rationale (network git-pull dwarfs the native latency budget).
	r.mu.RLock()
	if time.Since(r.lastPull) < pullDebounce {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	r.mu.RLock()
	if time.Since(r.lastPull) < pullDebounce {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	pullCtx, cancelPull := context.WithTimeout(ctx, 5*time.Second)
	pull := exec.CommandContext(pullCtx, "git", "-C", r.dir,
		"pull", "--rebase", "--autostash", "origin", "main")
	pull.Env = os.Environ()
	if out, err := pull.CombinedOutput(); err != nil {
		log.Printf("git pull %s failed (continuing): %s: %v",
			r.dir, bytes.TrimSpace(out), err)
	}
	cancelPull()
	r.mu.Lock()
	r.lastPull = time.Now()
	r.mu.Unlock()

	fi, err := os.Stat(r.path())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat: %w", err)
	}
	r.mu.RLock()
	same := fi.ModTime().Equal(r.mtime)
	r.mu.RUnlock()
	if same {
		return nil
	}
	return r.reparseLocked()
}

// Get returns the workflow by name, or (zero, false) if unknown. Not
// falling back to code_task here — callers can decide (create's derivation
// path uses code_task; explicit `workflow:` field on the request should
// reject an unknown name with 422 UNKNOWN_WORKFLOW).
func (r *WorkflowRegistry) Get(name string) (Workflow, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	wf, ok := r.workflows[name]
	return wf, ok
}

// DeriveFromOwnerMode picks a workflow name for a create request that did
// not specify --workflow. Priority:
//  1. TASKOWNERS-matching rule's `workflow:` field (if provided).
//  2. mode-based defaults (Code → code_task, Think → think_task, else
//     code_task).
// Callers pass the matched OwnerRule when auto_assign resolved one; when
// nil, only mode drives the choice. Mirrors Python cmd_create workflow
// derivation at commands.py::_derive_workflow (commands.py:475-509 range).
func (r *WorkflowRegistry) DeriveFromOwnerMode(mode string, matched *OwnerRule) string {
	if matched != nil && matched.Workflow != "" {
		return matched.Workflow
	}
	switch upper(mode) {
	case "CODE":
		return "code_task"
	case "THINK":
		return "think_task"
	default:
		return "code_task"
	}
}

// CustomWorkflow is the inline `workflow:"custom"` payload.
type CustomWorkflow struct {
	Stages []string       `json:"stages" yaml:"stages"`
	Gates  map[string]any `json:"gates" yaml:"gates"`
}

// CustomWorkflowFailure is one violation of the custom-workflow schema —
// used to build 422 failures[] entries in handleCreate.
type CustomWorkflowFailure struct {
	Check  string
	Detail string
}

// validTaskStatuses is the frozen set from models.py TaskStatus.
// Regenerating this by hand keeps Go independent of Python at runtime;
// contract test in Phase 3 verifies parity.
var validTaskStatuses = map[string]bool{
	"BACKLOG": true, "READY": true, "IN_PROGRESS": true, "IN_REVIEW": true,
	"DONE": true, "BLOCKED": true, "REOPENED": true, "AWAITING_APPROVAL": true,
	"TODO": true, "PLANNING": true, "BRIEFING": true, "DATA_REVIEW": true,
	"REVIEW": true, "MERGED": true, "SUPERSEDED": true, "WONT-DO": true,
	"CANCELLED": true,
	"ASSIGNED": true, "INVESTIGATING": true, "MITIGATED": true, "RESOLVED": true,
	"POSTMORTEM": true, "REFERENCE_RESEARCH": true, "REVISION": true, "QA_REVIEW": true,
	"CLASSIFIED": true, "DISCOVERED": true, "INTERROGATED": true, "FRAMED": true,
	"DRAFTED": true, "LOOP_REVIEWED": true,
}

// ValidateCustom checks a CustomWorkflow submitted at create time against
// the invariants Python's _build_custom_workflow (commands.py:296-331)
// enforces: first stage MUST be BACKLOG, last MUST be DONE, all stages MUST
// be recognised TaskStatus values, all gate keys MUST reference a stage in
// the list. Returns the full failure list (bulk aggregation matches Python
// which raises on the first — we return everything to match our own
// AdvanceGateFailure convention).
//
// Gate shape validation (review+verdict / file / field+required) is deferred
// to Phase 3 store code where the raw JSON is already available.
func ValidateCustom(cw *CustomWorkflow) []CustomWorkflowFailure {
	if cw == nil {
		return []CustomWorkflowFailure{{
			Check:  "INVALID_CUSTOM_WORKFLOW",
			Detail: "workflow=custom requires stages[] and (optionally) gates{}",
		}}
	}
	var out []CustomWorkflowFailure
	stages := cw.Stages
	if len(stages) == 0 {
		out = append(out, CustomWorkflowFailure{
			Check:  "INVALID_CUSTOM_WORKFLOW",
			Detail: "stages must not be empty (Python: --stages \"A,B,C\" is required)",
		})
		return out
	}
	// BACKLOG-first, DONE-last invariant.
	if upper(stages[0]) != "BACKLOG" {
		out = append(out, CustomWorkflowFailure{
			Check:  "INVALID_CUSTOM_WORKFLOW",
			Detail: fmt.Sprintf("first stage must be BACKLOG (got %q)", stages[0]),
		})
	}
	if upper(stages[len(stages)-1]) != "DONE" {
		out = append(out, CustomWorkflowFailure{
			Check:  "INVALID_CUSTOM_WORKFLOW",
			Detail: fmt.Sprintf("last stage must be DONE (got %q)", stages[len(stages)-1]),
		})
	}
	// All stage names must be in TaskStatus.
	stageSet := make(map[string]bool, len(stages))
	for _, s := range stages {
		u := upper(s)
		stageSet[u] = true
		if !validTaskStatuses[u] {
			out = append(out, CustomWorkflowFailure{
				Check:  "INVALID_CUSTOM_WORKFLOW",
				Detail: fmt.Sprintf("stage %q is not a known TaskStatus", s),
			})
		}
	}
	// Every gate key must reference a stage present in stages.
	for gateKey := range cw.Gates {
		if !stageSet[upper(gateKey)] {
			out = append(out, CustomWorkflowFailure{
				Check:  "INVALID_CUSTOM_WORKFLOW",
				Detail: fmt.Sprintf("gate stage %q not in stages %v", gateKey, stages),
			})
		}
	}
	return out
}

// AxRoot resolves the on-disk ax/ checkout root the server reads
// taskowners.yaml + workflows.yaml from. Same constant as store.axFSRoot;
// exported here so cmd/backlog-server can bootstrap the registries before
// the store package is imported.
const AxRoot = "/opt/apps/ax"
