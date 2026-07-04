package store

// CreateTask — native port of ax/backlogist/core/commands.py::cmd_create
// covering the MVP surface defined in docs/specs/backlogserver-native-post-
// tasks-create-t-1414/{brief,requirements,design,task_plan}.md.
//
// Deferred (falls back to /exec):
//   - --infer-edges (deterministic edge propose)
//   - --check-similar (fuzzy title matching)
//
// The transaction is a single write (SERIAL guard → INSERT tasks →
// INSERT audit_trail → optional parent DONE→REOPENED cascade → optional
// blocked_by → task_edges sync → commit → cache.Bump). Cross-DB
// client/project UUID validation runs BEFORE the tx opens with a 2s
// timeout; fail-open on timeout yields a CROSS_DB_UNREACHABLE advisory,
// fail-closed on reachable-404 yields a 422 INVALID_CLIENT_UUID.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/docean552-star/backlog-server/internal/config"
	"github.com/docean552-star/backlog-server/internal/ears"
	"github.com/docean552-star/backlog-server/internal/signals"
)

// CreateTaskRequest is the JSON body of POST /tasks. Field names match
// design.md §Data models. Slice fields (tags/consumers/blocked_by/references)
// accept either JSON arrays or comma-separated strings — the client
// dispatcher (Phase 5) already normalises, but we accept both shapes so a
// raw curl body works without CLI wrapping.
type CreateTaskRequest struct {
	Agent          string                  `json:"agent"`
	Title          string                  `json:"title"`
	Owner          string                  `json:"owner,omitempty"`
	Mode           string                  `json:"mode"`
	BusinessValue  string                  `json:"business_value,omitempty"`
	Why            string                  `json:"why,omitempty"`
	Note           string                  `json:"note,omitempty"`
	Type           string                  `json:"type,omitempty"`
	Session        string                  `json:"session,omitempty"`
	MigrationNote  string                  `json:"migration_note,omitempty"`
	Client         string                  `json:"client,omitempty"`
	Project        string                  `json:"project,omitempty"`
	Parent         int                     `json:"parent,omitempty"`
	PriorityOverride int                   `json:"priority_override,omitempty"`
	BlockedBy      []int                   `json:"blocked_by,omitempty"`
	Consumers      []string                `json:"consumers,omitempty"`
	Tags           []string                `json:"tags,omitempty"`
	References     []string                `json:"references,omitempty"`
	AutoAssign     bool                    `json:"auto_assign,omitempty"`
	Lightweight    bool                    `json:"lightweight,omitempty"`
	Template       string                  `json:"template,omitempty"`
	Workflow       string                  `json:"workflow,omitempty"`
	CustomWorkflow *config.CustomWorkflow  `json:"custom_workflow,omitempty"`
	SignalContext  *signals.SignalContext  `json:"signal_context,omitempty"`
}

// CreateTaskResult is the 200-body of POST /tasks (+ 422 body carries
// Failures instead of TaskID). Advisories accumulate every soft-note the
// pipeline emits (EARS advisory, cross-DB unreachable, signal-type unknown,
// owner suggestion, rescore failure, edges sync failure).
type CreateTaskResult struct {
	TaskID         int                       `json:"task_id,omitempty"`
	Status         string                    `json:"status,omitempty"`
	Workflow       string                    `json:"workflow,omitempty"`
	Owner          string                    `json:"owner,omitempty"`
	OwnerSource    string                    `json:"owner_source,omitempty"` // "explicit" | "taskowners" | ""
	ParentReopened bool                      `json:"parent_reopened,omitempty"`
	Failures       []AdvanceGateFailure      `json:"failures,omitempty"`
	Advisories     []CreateAdvisory          `json:"advisories,omitempty"`
}

// CreateAdvisory is a soft-note pair. Code is machine-readable, Message is
// operator-facing.
type CreateAdvisory struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SetConfigRegistries injects the TASKOWNERS + workflow registries into the
// Store. Called once from cmd/backlog-server/main.go after LoadTaskowners /
// LoadWorkflows return. Idempotent — safe to call twice.
func (s *Store) SetConfigRegistries(t *config.TaskownersRegistry, w *config.WorkflowRegistry) {
	s.taskowners = t
	s.workflows = w
}

// CreateTask performs the full create pipeline (validate → cross-DB →
// tx → INSERT tasks + audit_trail → parent cascade → edges sync → commit →
// cache.Bump). Returns 200-body (TaskID populated) on success; 422-body
// (Failures populated, TaskID=0) on any validation failure. Errors on
// infrastructure (DB unreachable, tx begin failed) surface via error.
//
// The pipeline collects ALL failures rather than fail-fast, matching the
// existing AdvanceTask convention (store.go:677-688 gate aggregation).
func (s *Store) CreateTask(ctx context.Context, req CreateTaskRequest) (CreateTaskResult, error) {
	// ─── Pre-tx: reload registries + collect failures ─────────────────
	if s.taskowners != nil {
		_ = s.taskowners.Reload(ctx) // best-effort; falls back to cached
	}
	if s.workflows != nil {
		_ = s.workflows.Reload(ctx)
	}

	failures := s.validate(&req)
	res := CreateTaskResult{}

	// EARS advisory on business_value — non-blocking (brief.md Q3 override).
	if req.Parent == 0 && strings.TrimSpace(req.BusinessValue) != "" &&
		!ears.MatchesEARS(req.BusinessValue) {
		res.Advisories = append(res.Advisories, CreateAdvisory{
			Code:    "EARS_ADVISORY_BV",
			Message: "business_value does not follow EARS syntax (WHEN/WHILE/WHERE/IF-THEN/ubiquitous SHALL). Advisory only, not a block.",
		})
	}

	// Cross-DB fail-open on client / project UUIDs.
	if len(failures) == 0 {
		crossFail, crossAdv := s.validateCrossDBRefs(ctx, req.Client, req.Project)
		failures = append(failures, crossFail...)
		res.Advisories = append(res.Advisories, crossAdv...)
	}

	// Owner resolution — populates req.Owner via TASKOWNERS if requested.
	ownerSource, workflowFromRule, ownerFailures, ownerAdvisories := s.resolveOwner(&req)
	failures = append(failures, ownerFailures...)
	res.Advisories = append(res.Advisories, ownerAdvisories...)

	if len(failures) > 0 {
		res.Failures = failures
		return res, nil
	}

	// Workflow derivation (may be overridden by --workflow flag).
	workflow := s.deriveWorkflow(&req, workflowFromRule)
	if req.Lightweight {
		workflow = "code_quick" // #947 B1: --lightweight wins over everything
	}

	// Signal context vocabulary check (soft advisory only).
	if req.SignalContext != nil {
		if !signals.IsKnown(req.SignalContext.IssueType_()) {
			res.Advisories = append(res.Advisories, CreateAdvisory{
				Code: "SIGNAL_CONTEXT_TYPE_UNKNOWN",
				Message: fmt.Sprintf("signal_context.type %q outside known receiver vocabulary — storing as-is (Go owns the truth via cron.go:1180)",
					req.SignalContext.IssueType_()),
			})
		}
	}

	// ─── Tx: SERIAL guard + INSERT tasks + audit_trail + parent cascade ─
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// SERIAL guard — mirrors SubtasksFromPlan store.go:2825-2830.
	if _, err := tx.Exec(ctx,
		`SELECT setval(pg_get_serial_sequence('tasks','id'),
		               COALESCE((SELECT MAX(id) FROM tasks), 1))`,
	); err != nil {
		return res, fmt.Errorf("serial guard: %w", err)
	}

	// Build custom_fields JSON with (custom_workflow + signal_context +
	// workflow_override). Keys match Python commands.py:625/639/652 verbatim.
	customFields := buildCustomFields(&req, workflow)
	customFieldsJSON, err := json.Marshal(customFields)
	if err != nil {
		return res, fmt.Errorf("marshal custom_fields: %w", err)
	}

	// Serialise JSON list columns.
	blockedByJSON, _ := json.Marshal(nonNilInts(req.BlockedBy))
	consumersJSON, _ := json.Marshal(nonNilStrings(req.Consumers))
	tagsJSON, _ := json.Marshal(nonNilStrings(req.Tags))
	referencesJSON, _ := json.Marshal(nonNilStrings(req.References))

	nowDate := time.Now().UTC().Format("2006-01-02")
	var newID int
	command := "create"
	if ownerSource == "taskowners" {
		command = "create (auto-assigned by TASKOWNERS)"
	} else if req.Parent > 0 {
		command = fmt.Sprintf("create (child of #%d)", req.Parent)
	}

	// Parent-not-found guard runs INSIDE the tx so a concurrent parent
	// cancel is caught. Mirrors SubtasksFromPlan parent-check pattern.
	if req.Parent > 0 {
		var parentStatus string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM tasks WHERE id = $1`, req.Parent,
		).Scan(&parentStatus); err != nil {
			if err == pgx.ErrNoRows {
				res.Failures = append(res.Failures, AdvanceGateFailure{
					Check:  "PARENT_NOT_FOUND",
					Detail: fmt.Sprintf("parent #%d not found", req.Parent),
				})
				return res, nil
			}
			return res, fmt.Errorf("check parent: %w", err)
		}
		if strings.EqualFold(parentStatus, "DONE") {
			// Cascade: reopen parent + emit audit row + set flag.
			if _, err := tx.Exec(ctx,
				`UPDATE tasks SET status='REOPENED', closed_at=NULL, updated_at=NOW()::text WHERE id=$1`,
				req.Parent,
			); err != nil {
				return res, fmt.Errorf("reopen parent: %w", err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
				 VALUES ($1, $2, 'status', 'DONE', 'REOPENED', 'create: parent reopened (child added)', NOW()::text)`,
				req.Parent, req.Agent,
			); err != nil {
				return res, fmt.Errorf("audit parent reopen: %w", err)
			}
			res.ParentReopened = true
		}
	}

	// INSERT tasks. Full column set covering all optional/JSON columns —
	// missing fields fall through to schema defaults so the INSERT stays
	// tolerant of a partially-populated request.
	if err := tx.QueryRow(ctx,
		`INSERT INTO tasks (
			title, why, owner, status, mode, type, note,
			business_value, session, migration_note,
			client_id, project_id, priority_override,
			blocked_by, consumers, tags, "references",
			workflow, custom_fields,
			parent_task_id,
			created, created_at, updated_at
		) VALUES (
			$1,$2,$3,'BACKLOG',$4,$5,$6,
			$7,$8,$9,
			NULLIF($10,''), NULLIF($11,''), $12,
			$13,$14,$15,$16,
			$17,$18,
			NULLIF($19,0),
			$20, NOW()::text, NOW()::text
		) RETURNING id`,
		req.Title, req.Why, req.Owner, req.Mode, nullIfEmpty(req.Type), req.Note,
		req.BusinessValue, req.Session, req.MigrationNote,
		req.Client, req.Project, req.PriorityOverride,
		string(blockedByJSON), string(consumersJSON), string(tagsJSON), string(referencesJSON),
		workflow, string(customFieldsJSON),
		req.Parent,
		nowDate,
	).Scan(&newID); err != nil {
		return res, fmt.Errorf("insert task: %w", err)
	}

	// Audit trail row per create. Column shape mirrors AdvanceTask
	// (store.go:734-738).
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', '', 'BACKLOG', $3, NOW()::text)`,
		newID, req.Agent, command,
	); err != nil {
		return res, fmt.Errorf("insert audit: %w", err)
	}

	// blocked_by → task_edges sync (fail-open per requirements AC 11.3).
	// task_edges is a separate normalisation store used by graph queries;
	// a missing row is not a fatal error, just an advisory.
	if len(req.BlockedBy) > 0 {
		if err := syncBlockedByToEdges(ctx, tx, newID, req.BlockedBy); err != nil {
			res.Advisories = append(res.Advisories, CreateAdvisory{
				Code:    "EDGES_SYNC_FAILED",
				Message: fmt.Sprintf("task_edges sync failed (%v) — dependencies still tracked in tasks.blocked_by", err),
			})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}

	// Post-commit rescore is a fire-and-forget advisory — a failed rescore
	// never rolls back the successful create.
	// (Actual rescore call omitted in MVP; wire when scoring package lands
	// in Go. brief.md task_plan Phase 3 last item.)

	res.TaskID = newID
	res.Status = "BACKLOG"
	res.Workflow = workflow
	res.Owner = req.Owner
	res.OwnerSource = ownerSource
	return res, nil
}

// ─── validation ────────────────────────────────────────────────────────

// validate collects required-field failures without hitting the DB. Returns
// the full failure list (bulk aggregation per design.md §Failure aggregation).
func (s *Store) validate(req *CreateTaskRequest) []AdvanceGateFailure {
	var out []AdvanceGateFailure

	if strings.TrimSpace(req.Agent) == "" {
		out = append(out, AdvanceGateFailure{
			Check: "AGENT_REQUIRED", Detail: "agent required in request body",
		})
	}
	if strings.TrimSpace(req.Title) == "" {
		out = append(out, AdvanceGateFailure{
			Check: "TITLE_REQUIRED", Detail: "title is required",
		})
	}
	if strings.TrimSpace(req.Mode) == "" {
		out = append(out, AdvanceGateFailure{
			Check: "MODE_REQUIRED", Detail: "mode is required (Code/Think/Integration/...)",
		})
	}
	if req.Parent == 0 && strings.TrimSpace(req.BusinessValue) == "" {
		out = append(out, AdvanceGateFailure{
			Check:  "BUSINESS_VALUE_REQUIRED",
			Detail: "business_value is required unless --parent is set",
		})
	}
	// Template validation (bugfix/feature/migration required-field check).
	tf := config.TemplateFields{
		References:    req.References,
		Consumers:     req.Consumers,
		MigrationNote: req.MigrationNote,
	}
	if fail := config.ValidateTemplate(req.Template, tf); fail != nil {
		out = append(out, AdvanceGateFailure{
			Check: fail.Check, Detail: fail.Detail,
		})
	}
	// Custom workflow schema check (only when workflow=="custom").
	if strings.EqualFold(req.Workflow, "custom") {
		for _, f := range config.ValidateCustom(req.CustomWorkflow) {
			out = append(out, AdvanceGateFailure{Check: f.Check, Detail: f.Detail})
		}
	} else if strings.TrimSpace(req.Workflow) != "" && s.workflows != nil {
		// Named workflow — verify it exists in workflows.yaml. Custom + empty
		// go the derivation path.
		if _, ok := s.workflows.Get(strings.ToLower(req.Workflow)); !ok {
			out = append(out, AdvanceGateFailure{
				Check:  "UNKNOWN_WORKFLOW",
				Detail: fmt.Sprintf("unknown workflow %q — use one from workflows.yaml or workflow=custom with stages", req.Workflow),
			})
		}
	}
	return out
}

// resolveOwner picks req.Owner via TASKOWNERS when the client did not
// provide one and auto_assign is set, or derives workflow from an existing
// owner via the same registry. Returns (ownerSource, workflowFromRule,
// failures, advisories).
func (s *Store) resolveOwner(req *CreateTaskRequest) (string, string, []AdvanceGateFailure, []CreateAdvisory) {
	if s.taskowners == nil {
		return "explicit", "", nil, nil
	}
	facts := config.TaskFacts{
		Title:     req.Title,
		Owner:     req.Owner,
		Mode:      req.Mode,
		Tags:      req.Tags,
		Consumers: req.Consumers,
	}
	if strings.TrimSpace(req.Owner) == "" {
		// TASKOWNERS lookup.
		if !req.AutoAssign {
			// No owner + no auto_assign: emit OWNER_SUGGESTION advisory
			// (partial-match list) and let validate() surface OWNER_REQUIRED
			// via a synthetic failure below.
			suggestions := s.taskowners.SuggestMatches(facts, 5)
			var adv []CreateAdvisory
			if len(suggestions) > 0 {
				adv = append(adv, CreateAdvisory{
					Code:    "OWNER_SUGGESTION",
					Message: fmt.Sprintf("no explicit owner + no --auto-assign; TASKOWNERS partial matches suggest: %s", strings.Join(suggestions, ", ")),
				})
			}
			return "", "", []AdvanceGateFailure{{
				Check:  "OWNER_REQUIRED",
				Detail: "owner required (use --auto-assign to let TASKOWNERS pick)",
			}}, adv
		}
		// Auto-assign path: match the best rule; empty → 422 NO_MATCHING_OWNER.
		rule := s.taskowners.MatchRule(facts, nil)
		if rule == nil {
			return "", "", []AdvanceGateFailure{{
				Check:  "NO_MATCHING_OWNER",
				Detail: "auto_assign requested but no TASKOWNERS rule matched",
			}}, nil
		}
		req.Owner = rule.Owner
		return "taskowners", rule.Workflow, nil, nil
	}
	// Owner given explicitly: still derive workflow from a matching rule if any.
	rule := s.taskowners.MatchRule(facts, nil)
	if rule != nil {
		return "explicit", rule.Workflow, nil, nil
	}
	return "explicit", "", nil, nil
}

// deriveWorkflow picks the workflow name from (rule, --workflow flag,
// mode-default) precedence. --workflow=custom short-circuits to "custom".
// Named --workflow wins over TASKOWNERS-derived; if empty, fall back to
// TASKOWNERS-derived or mode default.
func (s *Store) deriveWorkflow(req *CreateTaskRequest, workflowFromRule string) string {
	flag := strings.ToLower(strings.TrimSpace(req.Workflow))
	if flag == "custom" {
		return "custom"
	}
	if flag != "" {
		return flag // validated as registered by validate()
	}
	if workflowFromRule != "" {
		return workflowFromRule
	}
	if s.workflows == nil {
		if strings.EqualFold(req.Mode, "Think") {
			return "think_task"
		}
		return "code_task"
	}
	return s.workflows.DeriveFromOwnerMode(req.Mode, nil)
}

// validateCrossDBRefs pings AyantXAPI for the client/project UUIDs.
// Reachable-404 → 422 with per-check code. Timeout/5xx → CROSS_DB_UNREACHABLE
// advisory + accept the create (fail-open per requirements AC 9.6).
// Empty UUIDs are no-ops.
func (s *Store) validateCrossDBRefs(ctx context.Context, clientID, projectID string) ([]AdvanceGateFailure, []CreateAdvisory) {
	base := strings.TrimSpace(os.Getenv("AX_API_BASE"))
	if base == "" {
		// Config not wired → treat as unreachable but advisory only. The
		// operator can opt in by setting AX_API_BASE later without a code
		// change.
		var adv []CreateAdvisory
		if clientID != "" || projectID != "" {
			adv = append(adv, CreateAdvisory{
				Code:    "CROSS_DB_UNREACHABLE",
				Message: "AX_API_BASE unset — client/project UUID validation skipped",
			})
		}
		return nil, adv
	}
	var failures []AdvanceGateFailure
	var advisories []CreateAdvisory
	if clientID != "" {
		fail, adv := crossDBCheck(ctx, base, "clients", clientID, "INVALID_CLIENT_UUID")
		if fail != nil {
			failures = append(failures, *fail)
		}
		if adv != nil {
			advisories = append(advisories, *adv)
		}
	}
	if projectID != "" {
		fail, adv := crossDBCheck(ctx, base, "projects", projectID, "INVALID_PROJECT_UUID")
		if fail != nil {
			failures = append(failures, *fail)
		}
		if adv != nil {
			advisories = append(advisories, *adv)
		}
	}
	return failures, advisories
}

// crossDBCheck issues a 2s HEAD request. 200 → nil, 404 → failure,
// timeout/5xx → advisory (fail-open).
func crossDBCheck(ctx context.Context, base, resource, uuid, failCode string) (*AdvanceGateFailure, *CreateAdvisory) {
	url := fmt.Sprintf("%s/api/v1/%s/%s", strings.TrimRight(base, "/"), resource, uuid)
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pingCtx, http.MethodHead, url, nil)
	if err != nil {
		return nil, &CreateAdvisory{
			Code: "CROSS_DB_UNREACHABLE",
			Message: fmt.Sprintf("could not build %s check request: %v — accepting create",
				resource, err),
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &CreateAdvisory{
			Code: "CROSS_DB_UNREACHABLE",
			Message: fmt.Sprintf("%s check failed (%v) — accepting create",
				resource, err),
		}
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil, nil
	case resp.StatusCode == http.StatusNotFound:
		return &AdvanceGateFailure{
			Check:  failCode,
			Detail: fmt.Sprintf("%s %q not found in AyantXAPI", resource, uuid),
		}, nil
	case resp.StatusCode >= 500:
		return nil, &CreateAdvisory{
			Code:    "CROSS_DB_UNREACHABLE",
			Message: fmt.Sprintf("%s check returned %d — accepting create", resource, resp.StatusCode),
		}
	default:
		// 4xx that is not 404 (401 auth, 403 forbidden) — advisory but not
		// fail-closed. Operator sees the message + can decide.
		return nil, &CreateAdvisory{
			Code:    "CROSS_DB_UNREACHABLE",
			Message: fmt.Sprintf("%s check returned %d — accepting create", resource, resp.StatusCode),
		}
	}
}

// ─── custom_fields JSON assembly ───────────────────────────────────────

// buildCustomFields assembles the JSON blob stored in tasks.custom_fields.
// Keys are namespaced to match Python's commands.py:625/639/652 (custom_workflow,
// signal_context, workflow_override) so downstream advance / take / closure-
// reviewer read the exact keys they expect.
func buildCustomFields(req *CreateTaskRequest, workflow string) map[string]any {
	out := map[string]any{}
	if req.CustomWorkflow != nil && strings.EqualFold(req.Workflow, "custom") {
		out["custom_workflow"] = req.CustomWorkflow
	}
	if req.SignalContext != nil {
		out["signal_context"] = req.SignalContext
	}
	// workflow_override is set only when the client explicitly passed a
	// non-custom workflow name — persists the operator's choice so a later
	// derivation pass can honour it (Python commands.py:485-486).
	if req.Workflow != "" && !strings.EqualFold(req.Workflow, "custom") && workflow == strings.ToLower(strings.TrimSpace(req.Workflow)) {
		out["workflow_override"] = workflow
	}
	return out
}

// ─── blocked_by → task_edges sync ─────────────────────────────────────

// syncBlockedByToEdges inserts one task_edges row per blocker. Uses ON
// CONFLICT DO NOTHING for idempotency. Called inside the create tx so the
// rows commit atomically with the tasks INSERT.
func syncBlockedByToEdges(ctx context.Context, tx pgx.Tx, taskID int, blockers []int) error {
	for _, b := range blockers {
		if b <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_edges (from_task_id, to_task_id, edge_type)
			 VALUES ($1, $2, 'blocks')
			 ON CONFLICT DO NOTHING`,
			b, taskID,
		); err != nil {
			return err
		}
	}
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func nonNilInts(s []int) []int {
	if s == nil {
		return []int{}
	}
	return s
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
