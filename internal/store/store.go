package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/docean552-star/backlog-server/internal/cache"
)

// axFSRoot is the on-disk root of the ax/ checkout that server-side handlers
// read spec/plan files from. Matches proxy.go's execWorkDir. Handlers that
// touch files (subtasks-from-plan) resolve relative task.task_plan paths
// against this root. Server-side git pull (proxy.go:121-131) refreshes this
// tree before every /exec, so files pushed by agents are visible quickly.
const axFSRoot = "/opt/apps/ax"

// Active statuses recognised by Python `recommend_next` (recommendations.py:69).
var activeStatuses = map[string]bool{
	"TODO":        true,
	"IN_PROGRESS": true,
	"REOPENED":    true,
	"READY":       true,
	"PLANNING":    true,
}

// Terminal statuses — a blocker in any of these no longer blocks.
var terminalStatuses = map[string]bool{
	"DONE":       true,
	"CANCELLED":  true,
	"SUPERSEDED": true,
}

type Store struct {
	pool  *pgxpool.Pool
	cache *cache.Cache // nil → cache disabled, all reads go straight to PG
}

// New opens a pgx pool against dsn, optionally wired to a cache for read paths.
// Pass nil cache to run without caching (Phase 1 behaviour).
func New(ctx context.Context, dsn string, c *cache.Cache) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgxpool.Ping: %w", err)
	}
	s := &Store{pool: pool, cache: c}
	if err := s.installNotifyTrigger(ctx); err != nil {
		// Trigger install is best-effort: if the role lacks privilege we still
		// want the server to come up — cache will just rely on TTL for staleness.
		fmt.Printf("notify trigger install failed (continuing without it): %v\n", err)
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// DSN exposes the connection string so the notify subscriber can open its own
// long-lived connection (pgxpool can recycle conns; LISTEN needs persistence).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// cacheOrFetch is the standard read pattern: try cache, fall back to fetch, store
// on miss. Returns (value, hit, err). Errors from cache.Get/Set never block the
// fetch — the cache is best-effort.
func cacheOrFetch[T any](ctx context.Context, c *cache.Cache, suffix string, fetch func() (T, error)) (T, bool, error) {
	var got T
	if hit, _ := c.Get(ctx, suffix, &got); hit {
		return got, true, nil
	}
	v, err := fetch()
	if err != nil {
		var zero T
		return zero, false, err
	}
	_ = c.Set(ctx, suffix, v)
	return v, false, nil
}

// installNotifyTrigger creates the pg_notify trigger on the tasks table.
// Idempotent: CREATE OR REPLACE FUNCTION + DROP TRIGGER IF EXISTS + CREATE TRIGGER.
// Trigger emits NOTIFY tasks_changed with the row id on every INSERT/UPDATE/DELETE.
const notifyTriggerDDL = `
CREATE OR REPLACE FUNCTION notify_tasks_changed() RETURNS trigger AS $fn$
BEGIN
	PERFORM pg_notify('tasks_changed', COALESCE(NEW.id::text, OLD.id::text, ''));
	RETURN COALESCE(NEW, OLD);
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tasks_notify ON tasks;
CREATE TRIGGER tasks_notify
	AFTER INSERT OR UPDATE OR DELETE ON tasks
	FOR EACH ROW EXECUTE FUNCTION notify_tasks_changed();
`

func (s *Store) installNotifyTrigger(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, notifyTriggerDDL)
	return err
}

// Task is the row shape exposed by the read endpoints. JSON tags use snake_case
// matching the tasks table columns so clients can rely on stable names.
type Task struct {
	ID             int      `json:"id"`
	Title          string   `json:"title"`
	Why            string   `json:"why"`
	Owner          string   `json:"owner"`
	Status         string   `json:"status"`
	Mode           string   `json:"mode"`
	Workflow       string   `json:"workflow"`
	Type           *string  `json:"type"`
	EffectiveScore float64  `json:"effective_score"`
	BlockedBy      []int    `json:"blocked_by"`
	DoneWhen       []string `json:"done_when"`
	References     []string `json:"references"`
	TaskPlan       string   `json:"task_plan"`
	Spec           string   `json:"spec"`
	BusinessValue  string   `json:"business_value"`
	Note           string   `json:"note"`
	ParentTaskID   *int     `json:"parent_task_id"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
	ClosedAt       *string  `json:"closed_at"`
}

// taskColumns selects effective_score via ::text::float8 so we get Python-equivalent
// precision. PG stores it as REAL (float32); pgx would otherwise widen the float32
// bits to float64 (31.3 → 31.299999...), while Python psycopg2 reads the text
// representation ("31.3") and parses it to float64 (31.3000...0071). The round-trip
// cast forces Go onto the same code path, eliminating off-by-0.1 rank-score drift.
const taskColumns = `id, title, why, owner, status, mode, workflow, type,
	effective_score::text::float8 AS effective_score,
	blocked_by, done_when, "references", task_plan, spec,
	business_value, note, parent_task_id, created_at, updated_at, closed_at`

func scanTask(row pgx.Row) (Task, error) {
	var t Task
	var blockedByJSON, doneWhenJSON, refsJSON string
	err := row.Scan(
		&t.ID, &t.Title, &t.Why, &t.Owner, &t.Status, &t.Mode, &t.Workflow, &t.Type,
		&t.EffectiveScore, &blockedByJSON, &doneWhenJSON, &refsJSON, &t.TaskPlan, &t.Spec,
		&t.BusinessValue, &t.Note, &t.ParentTaskID, &t.CreatedAt, &t.UpdatedAt, &t.ClosedAt,
	)
	if err != nil {
		return Task{}, err
	}
	t.BlockedBy = parseIntArray(blockedByJSON)
	t.DoneWhen = parseStringArray(doneWhenJSON)
	t.References = parseStringArray(refsJSON)
	return t, nil
}

func parseIntArray(s string) []int {
	if s == "" {
		return []int{}
	}
	var out []int
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return []int{}
	}
	return out
}

func parseStringArray(s string) []string {
	if s == "" {
		return []string{}
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return []string{}
	}
	return out
}

// ListTasks returns tasks filtered by optional owner/status. Cached by (owner, status) key.
func (s *Store) ListTasks(ctx context.Context, owner, status string) ([]Task, bool, error) {
	suffix := fmt.Sprintf("tasks:owner=%s:status=%s", strings.ToLower(owner), strings.ToUpper(status))
	return cacheOrFetch(ctx, s.cache, suffix, func() ([]Task, error) {
		return s.queryListTasks(ctx, owner, status)
	})
}

func (s *Store) queryListTasks(ctx context.Context, owner, status string) ([]Task, error) {
	clauses := []string{}
	args := []any{}
	if owner != "" {
		args = append(args, strings.ToLower(owner))
		clauses = append(clauses, fmt.Sprintf("lower(owner) = $%d", len(args)))
	}
	if status != "" {
		args = append(args, strings.ToUpper(status))
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	q := fmt.Sprintf("SELECT %s FROM tasks %s ORDER BY id", taskColumns, where)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Task{}
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetTask returns one task or pgx.ErrNoRows if absent. Absences are NOT cached.
func (s *Store) GetTask(ctx context.Context, id int) (Task, bool, error) {
	suffix := fmt.Sprintf("task:%d", id)
	return cacheOrFetch(ctx, s.cache, suffix, func() (Task, error) {
		q := fmt.Sprintf("SELECT %s FROM tasks WHERE id = $1", taskColumns)
		return scanTask(s.pool.QueryRow(ctx, q, id))
	})
}

// IsNotFound reports whether err means the task was not found.
func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// StatusCounts returns count of tasks grouped by status.
func (s *Store) StatusCounts(ctx context.Context) (map[string]int, bool, error) {
	return cacheOrFetch(ctx, s.cache, "status_counts", func() (map[string]int, error) {
		rows, err := s.pool.Query(ctx, "SELECT status, COUNT(*) FROM tasks GROUP BY status")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[string]int{}
		for rows.Next() {
			var status string
			var n int
			if err := rows.Scan(&status, &n); err != nil {
				return nil, err
			}
			out[status] = n
		}
		return out, rows.Err()
	})
}

// Candidate is a single entry in the /next response.
type Candidate struct {
	ID             int     `json:"id"`
	Title          string  `json:"title"`
	RankScore      float64 `json:"rank_score"`
	EffectiveScore float64 `json:"effective_score"`
	Readiness      float64 `json:"readiness"`
	Reason         string  `json:"reason"`
	TaskPlan       string  `json:"task_plan"`
	Status         string  `json:"status"`
}

// NextResult mirrors the shape of Python recommend_next().
type NextResult struct {
	Candidates []Candidate `json:"candidates"`
	Blocked    []string    `json:"blocked"`
	Conflicts  []string    `json:"conflicts"`
}

// NextForAgent ports recommend_next from ax/backlogist/core/recommendations.py:23.
//
// Deviation from Python: readiness FS-checks on task_plan/spec are replaced with
// field-non-empty proxy (DESIGN.md §6.4) because the central server cannot see
// clients' local filesystems.
func (s *Store) NextForAgent(ctx context.Context, agent string, limit int) (NextResult, bool, error) {
	if limit <= 0 {
		limit = 5
	}
	agent = strings.ToLower(agent)
	suffix := fmt.Sprintf("next:%s:%d", agent, limit)
	return cacheOrFetch(ctx, s.cache, suffix, func() (NextResult, error) {
		return s.computeNextForAgent(ctx, agent, limit)
	})
}

func (s *Store) computeNextForAgent(ctx context.Context, agent string, limit int) (NextResult, error) {
	all, err := s.queryListTasks(ctx, "", "")
	if err != nil {
		return NextResult{}, err
	}

	unblocks := map[int]int{}
	for _, t := range all {
		for _, bid := range t.BlockedBy {
			unblocks[bid]++
		}
	}

	byID := map[int]Task{}
	for _, t := range all {
		byID[t.ID] = t
	}

	res := NextResult{Candidates: []Candidate{}, Blocked: []string{}, Conflicts: []string{}}
	agentTasks := []Task{}

	for _, t := range all {
		if strings.ToLower(t.Owner) != agent {
			continue
		}
		agentTasks = append(agentTasks, t)
		st := strings.ToUpper(t.Status)
		if !activeStatuses[st] {
			continue
		}

		activeBlockers := []int{}
		for _, bid := range t.BlockedBy {
			b, ok := byID[bid]
			if !ok {
				continue
			}
			if !terminalStatuses[strings.ToUpper(b.Status)] {
				activeBlockers = append(activeBlockers, bid)
			}
		}
		if len(activeBlockers) > 0 {
			ids := make([]string, len(activeBlockers))
			for i, b := range activeBlockers {
				ids[i] = fmt.Sprintf("#%d", b)
			}
			res.Blocked = append(res.Blocked, fmt.Sprintf("#%d (blocked by %s)", t.ID, strings.Join(ids, ", ")))
			continue
		}

		readiness := computeReadiness(t)
		readiness = min1(readiness + 0.1*float64(unblocks[t.ID]))
		rank := round1(t.EffectiveScore * (1 + readiness))

		reasons := []string{}
		if t.TaskPlan != "" {
			reasons = append(reasons, "has task_plan")
		}
		if st == "IN_PROGRESS" {
			reasons = append(reasons, "in progress")
		}
		if st == "REOPENED" {
			reasons = append(reasons, "reopened")
		}
		if n := unblocks[t.ID]; n > 0 {
			reasons = append(reasons, fmt.Sprintf("unblocks %d task(s)", n))
		}
		reason := "ready"
		if len(reasons) > 0 {
			reason = strings.Join(reasons, ", ")
		}

		res.Candidates = append(res.Candidates, Candidate{
			ID:             t.ID,
			Title:          t.Title,
			RankScore:      rank,
			EffectiveScore: t.EffectiveScore,
			Readiness:      readiness,
			Reason:         reason,
			TaskPlan:       t.TaskPlan,
			Status:         st,
		})
	}

	sort.Slice(res.Candidates, func(i, j int) bool {
		return res.Candidates[i].RankScore > res.Candidates[j].RankScore
	})
	if len(res.Candidates) > limit {
		res.Candidates = res.Candidates[:limit]
	}

	res.Conflicts = detectConflicts(agentTasks, all)
	return res, nil
}

// computeReadiness mirrors _compute_readiness (recommendations.py:129). FS-checks
// on task_plan / spec are replaced with field-non-empty proxy (see DESIGN.md §6.4).
func computeReadiness(t Task) float64 {
	score := 0.0
	if t.TaskPlan != "" {
		score += 0.3
	}
	if len(t.DoneWhen) > 0 {
		score += 0.2
	}
	if t.Spec != "" {
		score += 0.2
	}
	if len(t.References) > 0 {
		score += 0.1
	}
	st := strings.ToUpper(t.Status)
	if st == "REOPENED" {
		score += 0.2
	}
	if st == "IN_PROGRESS" {
		score += 0.3
	}
	return min1(score)
}

// detectConflicts mirrors _detect_conflicts (recommendations.py:162).
func detectConflicts(agentTasks, all []Task) []string {
	agentIDs := map[int]bool{}
	for _, t := range agentTasks {
		agentIDs[t.ID] = true
	}
	terminal := map[string]bool{"DONE": true, "CANCELLED": true, "SUPERSEDED": true, "BLOCKED": true}
	fileToTasks := map[string][]int{}
	for _, t := range all {
		if terminal[strings.ToUpper(t.Status)] {
			continue
		}
		for _, ref := range t.References {
			if strings.Contains(ref, ".") {
				fileToTasks[ref] = append(fileToTasks[ref], t.ID)
			}
		}
	}
	out := []string{}
	for path, ids := range fileToTasks {
		if len(ids) < 2 {
			continue
		}
		hit := false
		for _, id := range ids {
			if agentIDs[id] {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		strIDs := make([]string, len(ids))
		for i, id := range ids {
			strIDs[i] = fmt.Sprintf("#%d", id)
		}
		out = append(out, fmt.Sprintf("%s both modify %s", strings.Join(strIDs, ", "), path))
	}
	sort.Strings(out)
	return out
}

func min1(x float64) float64 {
	if x > 1.0 {
		return 1.0
	}
	return x
}

func round1(x float64) float64 {
	// One decimal place, matching Python `round(x, 1)`.
	return float64(int64(x*10+0.5)) / 10
}

// ---------------------------------------------------------------------------
// Advance — status transition with server-side DB gate checks
// ---------------------------------------------------------------------------

// AdvanceGateFailure is one item returned when a DB gate check blocks advance.
// File-based gate checks (research.md content, task_plan KQ/TS count, agent
// markers) stay on the client — the server has no local FS access to specs.
type AdvanceGateFailure struct {
	Check  string `json:"check"`
	Detail string `json:"detail"`
}

// codeTaskAdvanceMap is the linear code_task lifecycle: BACKLOG → PLANNING →
// READY → IN_PROGRESS → IN_REVIEW → AWAITING_APPROVAL → DONE.
var codeTaskAdvanceMap = map[string]string{
	"BACKLOG":           "PLANNING",
	"PLANNING":          "READY",
	"READY":             "IN_PROGRESS",
	"IN_PROGRESS":       "IN_REVIEW",
	"IN_REVIEW":         "AWAITING_APPROVAL",
	"AWAITING_APPROVAL": "DONE",
}

// ComputeAdvanceTarget returns the next status for a code_task advance, or ""
// if no advance is defined. Non-code workflows (think_task, marketing, seo, …)
// aren't handled server-side yet — they still go through the /exec subprocess
// proxy. MVP covers code_task only; the other workflows land in follow-ups.
func ComputeAdvanceTarget(currentStatus, workflow string) string {
	wf := strings.ToLower(strings.TrimSpace(workflow))
	if wf != "" && wf != "code_task" {
		return ""
	}
	return codeTaskAdvanceMap[strings.ToUpper(currentStatus)]
}

// HasLatestVerdict returns true iff a review_results row exists for this task
// with the given reviewer_model + verdict AND is_latest=true (matches the
// Python helper in backlogist/core/transition_gates.py::_has_review_verdict).
func (s *Store) HasLatestVerdict(ctx context.Context, taskID int, reviewer, verdict string) (bool, error) {
	var ok bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM review_results
			 WHERE task_id = $1
			   AND reviewer_model = $2
			   AND verdict = $3
			   AND is_latest = true
		)`,
		taskID, reviewer, verdict,
	).Scan(&ok)
	return ok, err
}

// getRequiredReviews mirrors Python transition_gates._get_required_reviews:
// priority 1 = custom_fields.required_reviews (CSV set at cmd_take), priority 2
// = mode-based defaults. Priority 2 (workflow.review_agents lookup) requires
// porting the workflows config module — deferred; the cache is set at cmd_take
// for every current task, so P1 hits in practice.
func (s *Store) getRequiredReviews(ctx context.Context, taskID int, mode string) ([]string, error) {
	var cfJSON string
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(custom_fields, '{}') FROM tasks WHERE id = $1`, taskID,
	).Scan(&cfJSON); err != nil {
		return nil, fmt.Errorf("load custom_fields: %w", err)
	}
	var cf map[string]any
	if err := json.Unmarshal([]byte(cfJSON), &cf); err == nil {
		if raw, ok := cf["required_reviews"].(string); ok && strings.TrimSpace(raw) != "" {
			parts := strings.Split(raw, ",")
			out := make([]string, 0, len(parts))
			for _, p := range parts {
				if t := strings.TrimSpace(p); t != "" {
					out = append(out, t)
				}
			}
			if len(out) > 0 {
				return out, nil
			}
		}
	}
	// Mode-based defaults (Python parity, transition_gates.py:2392-2398).
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "code":
		return []string{"code-reviewer", "task-closure-reviewer"}, nil
	case "think":
		return []string{"spec-reviewer", "task-closure-reviewer"}, nil
	default:
		return []string{"task-closure-reviewer"}, nil
	}
}

// AdvanceResult is the payload the /advance handler returns to the client.
type AdvanceResult struct {
	Task       Task                 `json:"task"`
	FromStatus string               `json:"from_status"`
	ToStatus   string               `json:"to_status"`
	Failures   []AdvanceGateFailure `json:"failures,omitempty"`
}

// AdvanceTask runs DB-side gate checks and, on pass, UPDATE status +
// INSERT audit_trail atomically. Returns:
//   - result populated with task + failures if any gate failed (no update happened)
//   - pgx.ErrNoRows if the task doesn't exist
//   - any other error from the DB
//
// Gate coverage (code_task workflow only):
//   - PLANNING → READY: done_when non-empty + latest spec-reviewer PASS verdict.
//   - READY → IN_PROGRESS: pass through with audit_trail only. Python's gate
//     is entirely FS-based (test_oracle git commits, task_plan KQ parse,
//     brief.md). Callers who want the full check chain use /exec. Canonical
//     path for code_task is `take` (atomic status+owner).
//   - IN_PROGRESS → IN_REVIEW: latest code-reviewer PASS verdict (load-bearing
//     DB gate). FS-based checks (git commits mentioning task ID, test file
//     existence, refactor commit reminder, copy scan) stay in /exec.
//   - IN_REVIEW → AWAITING_APPROVAL: all required_reviews reviewers have a
//     latest PASS verdict. required_reviews resolved from custom_fields.
//     required_reviews (CSV set at cmd_take) with mode-based fallback.
//   - AWAITING_APPROVAL → DONE: caller must set approve=true (mirrors the CLI's
//     --approve flag, i.e. operator explicit consent) AND task must have a
//     latest task-closure-reviewer PASS verdict. Also sets closed_at=NOW().
//
// File-based checks (research.md content quality, task_plan KQ/TS count, agent
// markers, git log, test file globs) remain a client-side concern — the server
// has no local FS access. Native path returning 200 does NOT guarantee full
// Python parity; callers who want the full check chain can invoke /exec.
func (s *Store) AdvanceTask(ctx context.Context, taskID int, agent string, approve bool) (AdvanceResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return AdvanceResult{}, err
	}
	fromStatus := strings.ToUpper(strings.TrimSpace(task.Status))

	to := ComputeAdvanceTarget(fromStatus, task.Workflow)
	if to == "" {
		return AdvanceResult{
			Task:       task,
			FromStatus: fromStatus,
			ToStatus:   "",
			Failures: []AdvanceGateFailure{{
				Check:  "WORKFLOW",
				Detail: fmt.Sprintf("no server-side advance defined for status=%s workflow=%q (MVP covers code_task only)", fromStatus, task.Workflow),
			}},
		}, nil
	}

	// Server-side DB gates.
	var failures []AdvanceGateFailure
	if fromStatus == "PLANNING" && to == "READY" {
		if len(task.DoneWhen) == 0 {
			failures = append(failures, AdvanceGateFailure{
				Check:  "DONE_WHEN",
				Detail: fmt.Sprintf("done_when is empty. Run: backlogist #%d update done_when:\"operator sees X, tests pass, …\"", taskID),
			})
		}
		hasPass, err := s.HasLatestVerdict(ctx, taskID, "spec-reviewer", "PASS")
		if err != nil {
			return AdvanceResult{}, fmt.Errorf("verdict lookup: %w", err)
		}
		if !hasPass {
			failures = append(failures, AdvanceGateFailure{
				Check:  "SPEC_REVIEW",
				Detail: fmt.Sprintf("no latest spec-reviewer PASS verdict. Run @spec-reviewer, then: backlogist review-submit #%d --agent spec-reviewer --verdict PASS", taskID),
			})
		}
	}
	// READY → IN_PROGRESS: no DB gate for code_task.
	// Python's _check_to_in_progress is entirely FS-based (test_oracle git
	// commits, task_plan KQ parse, brief.md existence per workflow) — none of
	// these can be done cleanly from the server without either mounting the ax/
	// tree read/write or duplicating the git-log convention. Callers who want
	// the full check chain use /exec. Canonical path for code_task is `take`,
	// not advance from READY. Left as no-op to keep the transition atomic in
	// audit_trail; parity gap documented in the function godoc.
	// IN_PROGRESS → IN_REVIEW: load-bearing DB gate = latest code-reviewer PASS.
	// Python also checks git commits, test files, refactor reminder, copy scan
	// — those stay on /exec (FS/git dependent). Native focuses on the one gate
	// that can be cleanly resolved from DB alone.
	if fromStatus == "IN_PROGRESS" && to == "IN_REVIEW" {
		hasPass, err := s.HasLatestVerdict(ctx, taskID, "code-reviewer", "PASS")
		if err != nil {
			return AdvanceResult{}, fmt.Errorf("verdict lookup: %w", err)
		}
		if !hasPass {
			failures = append(failures, AdvanceGateFailure{
				Check:  "CODE_REVIEW",
				Detail: fmt.Sprintf("no latest code-reviewer PASS verdict. Run @code-reviewer, then: backlogist review-submit #%d --agent code-reviewer --verdict PASS", taskID),
			})
		}
	}
	// IN_REVIEW → AWAITING_APPROVAL: all required_reviews reviewers PASS.
	// required_reviews resolved from custom_fields.required_reviews (CSV set at
	// cmd_take), with mode-based fallback (Code → code-reviewer + closure;
	// Think → spec-reviewer + closure; else → closure).
	if fromStatus == "IN_REVIEW" && to == "AWAITING_APPROVAL" {
		required, err := s.getRequiredReviews(ctx, taskID, task.Mode)
		if err != nil {
			return AdvanceResult{}, err
		}
		for _, reviewer := range required {
			hasPass, err := s.HasLatestVerdict(ctx, taskID, reviewer, "PASS")
			if err != nil {
				return AdvanceResult{}, fmt.Errorf("verdict lookup %s: %w", reviewer, err)
			}
			if !hasPass {
				failures = append(failures, AdvanceGateFailure{
					Check:  "REVIEW",
					Detail: fmt.Sprintf("%s verdict missing or not PASS. Run @%s, then: backlogist review-submit #%d --agent %s --verdict PASS", reviewer, reviewer, taskID, reviewer),
				})
			}
		}
	}
	// AWAITING_APPROVAL → DONE: closure gate. This one is load-bearing — plain
	// advance would previously walk past it silently (A's #1324 empirical
	// observation, samvel-32). Two independent checks (both must pass):
	//   - approve=true from the caller (proxy for the operator's --approve flag)
	//   - latest task-closure-reviewer PASS verdict
	if fromStatus == "AWAITING_APPROVAL" && to == "DONE" {
		if !approve {
			failures = append(failures, AdvanceGateFailure{
				Check:  "APPROVE",
				Detail: fmt.Sprintf("operator --approve required for AWAITING_APPROVAL → DONE (use: backlogist #%d advance --approve)", taskID),
			})
		}
		hasPass, err := s.HasLatestVerdict(ctx, taskID, "task-closure-reviewer", "PASS")
		if err != nil {
			return AdvanceResult{}, fmt.Errorf("verdict lookup: %w", err)
		}
		if !hasPass {
			failures = append(failures, AdvanceGateFailure{
				Check:  "CLOSURE_REVIEW",
				Detail: fmt.Sprintf("no latest task-closure-reviewer PASS verdict. Run @task-closure-reviewer, then: backlogist review-submit #%d --agent task-closure-reviewer --verdict PASS", taskID),
			})
		}
	}
	if len(failures) > 0 {
		return AdvanceResult{Task: task, FromStatus: fromStatus, ToStatus: to, Failures: failures}, nil
	}

	// Passed. UPDATE tasks + INSERT audit_trail atomically.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after commit

	// closed_at is set on the DONE transition. For all other targets the column
	// stays untouched (NULL on new rows, previously set on cancel/supersede
	// remains).
	updateSQL := `UPDATE tasks SET status = $1, updated_at = NOW()::text WHERE id = $2`
	if to == "DONE" {
		updateSQL = `UPDATE tasks SET status = $1, updated_at = NOW()::text, closed_at = NOW()::text WHERE id = $2`
	}
	if _, err := tx.Exec(ctx, updateSQL, to, taskID); err != nil {
		return AdvanceResult{}, fmt.Errorf("update tasks: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, $4, 'advance', NOW()::text)`,
		taskID, agent, fromStatus, to,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("insert audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceResult{}, fmt.Errorf("commit: %w", err)
	}

	// Invalidate reads (task cache + status counts + tasks lists + next).
	// Simple + safe: bump the version prefix, all previous keys become
	// unreachable and age out via TTL. Also handled by the LISTEN/NOTIFY
	// subscriber, but do it explicitly here so the response the client sees
	// immediately after advance is a fresh read.
	if s.cache != nil {
		s.cache.Bump()
	}

	// Refetch so updated_at / closed_at / any DB-side trigger effects appear in
	// the response. Fallback to in-memory mutation if the refetch fails (edge
	// case: DB unreachable right after commit) — status is the truth we
	// committed even if the read fails.
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = to
	}
	return AdvanceResult{Task: updated, FromStatus: fromStatus, ToStatus: to}, nil
}

// ---------------------------------------------------------------------------
// Take / Release — status + owner updates with audit
// ---------------------------------------------------------------------------

// TakeResult / ReleaseResult reuse AdvanceResult shape for consistency.

// Statuses from which `take` can move a task to IN_PROGRESS. Mirrors the
// validate_transition() rules for code_task in backlogist/core/workflows.py.
// Non-code workflows may allow different sources; MVP applies the same set
// broadly — advance-gate errors will surface any real conflicts.
var takeableStatuses = map[string]bool{
	"BACKLOG":     true,
	"PLANNING":    true,
	"READY":       true,
	"REOPENED":    true,
	"IN_PROGRESS": true, // idempotent: taking your own IN_PROGRESS task = no-op
	"TODO":        true,
}

// TakeTask sets status=IN_PROGRESS + owner=agent + audit. Does NOT populate
// custom_fields.required_agents/reviews (Python's cmd_take does — needs
// TASKOWNERS registry ported to Go). Does NOT start a time_entry or emit bot
// events. Those niceties stay on the /exec path until we port them.
func (s *Store) TakeTask(ctx context.Context, taskID int, agent string) (AdvanceResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return AdvanceResult{}, err
	}
	from := strings.ToUpper(strings.TrimSpace(task.Status))
	if !takeableStatuses[from] {
		return AdvanceResult{
			Task: task, FromStatus: from, ToStatus: "IN_PROGRESS",
			Failures: []AdvanceGateFailure{{
				Check:  "TRANSITION",
				Detail: fmt.Sprintf("cannot take task in status %s (must be one of BACKLOG/PLANNING/READY/REOPENED/TODO)", from),
			}},
		}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	oldOwner := task.Owner
	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'IN_PROGRESS', owner = $1, updated_at = NOW()::text WHERE id = $2`,
		agent, taskID,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'IN_PROGRESS', 'take', NOW()::text)`,
		taskID, agent, from,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("audit status: %w", err)
	}
	if oldOwner != agent {
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'owner', $3, $4, 'take', NOW()::text)`,
			taskID, agent, oldOwner, agent,
		); err != nil {
			return AdvanceResult{}, fmt.Errorf("audit owner: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = "IN_PROGRESS"
		updated.Owner = agent
	}
	return AdvanceResult{Task: updated, FromStatus: from, ToStatus: "IN_PROGRESS"}, nil
}

// Terminal statuses — cancel is a no-op / error on these because the task is done.
var cancelTerminalStatuses = map[string]bool{
	"DONE":       true,
	"CANCELLED":  true,
	"SUPERSEDED": true,
	"WONT-DO":    true,
	"MERGED":     true,
}

// CancelTask sets status=CANCELLED + audit + closed_at.
// Errors with a TRANSITION failure if the task is already terminal.
func (s *Store) CancelTask(ctx context.Context, taskID int, agent, reason string) (AdvanceResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return AdvanceResult{}, err
	}
	from := strings.ToUpper(strings.TrimSpace(task.Status))
	if cancelTerminalStatuses[from] {
		return AdvanceResult{
			Task: task, FromStatus: from, ToStatus: "CANCELLED",
			Failures: []AdvanceGateFailure{{
				Check:  "TRANSITION",
				Detail: fmt.Sprintf("task is already %s — no-op", from),
			}},
		}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'CANCELLED', updated_at = NOW()::text, closed_at = NOW()::text WHERE id = $1`,
		taskID,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("update: %w", err)
	}
	logReason := "cancel"
	if reason != "" {
		logReason = "cancel: " + reason
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'CANCELLED', $4, NOW()::text)`,
		taskID, agent, from, logReason,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = "CANCELLED"
	}
	return AdvanceResult{Task: updated, FromStatus: from, ToStatus: "CANCELLED"}, nil
}

// supersedeTerminalStatuses: statuses from which supersede is refused. Mirrors
// _TERMINAL_STATUSES in backlogist/core/commands.py:1721 — importantly DIFFERS
// from cancelTerminalStatuses in that DONE is NOT terminal here (Python allows
// DONE → SUPERSEDED; see test_cancel_supersede.py::test_b2).
var supersedeTerminalStatuses = map[string]bool{
	"CANCELLED":  true,
	"SUPERSEDED": true,
	"WONT-DO":    true,
	"MERGED":     true,
}

// SupersedeResult carries the same shape as AdvanceResult plus the replacement id.
type SupersedeResult struct {
	Task       Task                 `json:"task"`
	FromStatus string               `json:"from_status"`
	ToStatus   string               `json:"to_status"`
	ByID       int                  `json:"by_id"`
	Failures   []AdvanceGateFailure `json:"failures,omitempty"`
}

// SupersedeTask marks taskID as SUPERSEDED by byID: UPDATE tasks
// (status='SUPERSEDED', note="Superseded by #{byID}", closed_at=NOW()) +
// INSERT audit_trail (command="supersede --by #{byID}") atomically.
//
// Semantics ported from backlogist/core/commands.py::cmd_supersede:
//   - byID must reference an existing task (422 REPLACEMENT if missing).
//   - DONE is a valid source status (unlike cancel).
//   - note is rewritten to "Superseded by #{byID}" verbatim.
//
// Not ported: _warn_blocked_dependents (stderr hint about dependents whose
// blocked_by still references this task). Callers can enumerate them with
// `backlogist search blocked_by:#{id}` after the fact.
//
// export_yaml() is also not called — the Python CLI regenerates local
// backlog.yaml on write; native callers work through HTTP and don't need it.
func (s *Store) SupersedeTask(ctx context.Context, taskID int, agent string, byID int) (SupersedeResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return SupersedeResult{}, err
	}
	from := strings.ToUpper(strings.TrimSpace(task.Status))
	if supersedeTerminalStatuses[from] {
		return SupersedeResult{
			Task: task, FromStatus: from, ToStatus: "SUPERSEDED", ByID: byID,
			Failures: []AdvanceGateFailure{{
				Check:  "TRANSITION",
				Detail: fmt.Sprintf("task is already %s — cannot supersede", from),
			}},
		}, nil
	}
	// Replacement must exist. 422 REPLACEMENT (not 404) — the target task
	// itself is fine; it's the --by argument that points nowhere.
	if _, _, err := s.GetTask(ctx, byID); err != nil {
		if IsNotFound(err) {
			return SupersedeResult{
				Task: task, FromStatus: from, ToStatus: "SUPERSEDED", ByID: byID,
				Failures: []AdvanceGateFailure{{
					Check:  "REPLACEMENT",
					Detail: fmt.Sprintf("replacement task #%d not found", byID),
				}},
			}, nil
		}
		return SupersedeResult{}, fmt.Errorf("lookup replacement: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SupersedeResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	newNote := fmt.Sprintf("Superseded by #%d", byID)
	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'SUPERSEDED', note = $1, updated_at = NOW()::text, closed_at = NOW()::text WHERE id = $2`,
		newNote, taskID,
	); err != nil {
		return SupersedeResult{}, fmt.Errorf("update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'SUPERSEDED', $4, NOW()::text)`,
		taskID, agent, from, fmt.Sprintf("supersede --by #%d", byID),
	); err != nil {
		return SupersedeResult{}, fmt.Errorf("audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SupersedeResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = "SUPERSEDED"
		updated.Note = newNote
	}
	return SupersedeResult{Task: updated, FromStatus: from, ToStatus: "SUPERSEDED", ByID: byID}, nil
}

// reviewVerdictSet is the CLI-allowed verdict set (parity with
// cmd_review_submit in ax/backlogist/core/review_submit.py:50).
//
// NOTE ON SEMANTIC DRIFT: review_results.verdict has a schema CHECK constraint
// of {PASS, FAIL, NEEDS_WORK} (schema_postgres.sql:201). ACCEPT will fail the
// CHECK on INSERT and surface as a 500. REOPEN is remapped to NEEDS_WORK for
// the review_results row but preserved verbatim in audit_trail.new_value so
// downstream readers (sync_aggregate_state trigger, closure-reviewer) can
// disambiguate. Both behaviours mirror the pre-existing Python code exactly.
var reviewVerdictSet = map[string]bool{
	"PASS":       true,
	"ACCEPT":     true, // CLI-allowed; will hit schema CHECK on INSERT (Python parity)
	"NEEDS_WORK": true,
	"FAIL":       true,
	"REOPEN":     true, // stored as NEEDS_WORK in review_results.verdict, kept as REOPEN in audit_trail
}

// IsValidReviewVerdict reports whether v is one of the CLI-allowed verdicts.
// Exported so the handler can 400 before the transaction opens.
func IsValidReviewVerdict(v string) bool { return reviewVerdictSet[strings.ToUpper(strings.TrimSpace(v))] }

// ReviewSubmitResult is what SubmitReview returns to the handler.
type ReviewSubmitResult struct {
	TaskID      int    `json:"task_id"`
	Caller      string `json:"caller"`   // AX_AGENT env / audit_trail.agent (who submitted)
	Reviewer    string `json:"reviewer"` // review_results.reviewer_model (whose verdict)
	Verdict     string `json:"verdict"`  // CLI-level verdict (may be REOPEN even when review_results row stores NEEDS_WORK)
	IsAggregate bool   `json:"is_aggregate"`
	ReviewID    int64  `json:"review_id"`
	Summary     string `json:"summary"`
}

// SubmitReview atomically records a reviewer verdict for taskID:
//   1. Marks all prior (task_id, reviewer_model) rows in review_results as
//      is_latest=FALSE.
//   2. INSERTs a new review_results row with is_latest=TRUE (schema default,
//      set by the migration in ax/backlogist/storage/db.py:_run_is_latest_migration).
//   3. INSERTs an audit_trail row. field_changed='review' for a regular
//      reviewer submit, 'aggregate_review_verdict' when isAggregate — the
//      latter drives the sync_aggregate_state PG trigger (see #981).
//
// Callers: `caller` is the CLI operator (audit.agent), `reviewer` is the
// reviewer model whose verdict this is (review_results.reviewer_model). These
// are almost always different values (e.g. caller=samvel, reviewer=code-reviewer).
//
// Not ported from Python cmd_review_submit:
//   - _check_review_patterns (3 situational advisories — rate, self-review,
//     batch closure). They read cross-table state; cost/complexity high, MVP
//     leaves them client-side.
//   - Next-steps hints (still-needed reviews, «→ update status:IN_REVIEW»).
//     Uses transition_gates._get_required_reviews which isn't in Go yet.
func (s *Store) SubmitReview(ctx context.Context, taskID int, caller, reviewer, verdict, summary string, isAggregate bool) (ReviewSubmitResult, error) {
	if _, _, err := s.GetTask(ctx, taskID); err != nil {
		return ReviewSubmitResult{}, err
	}
	verdict = strings.ToUpper(strings.TrimSpace(verdict))
	if summary == "" {
		summary = fmt.Sprintf("%s review: %s", reviewer, verdict)
	}
	dbVerdict := verdict
	if verdict == "REOPEN" {
		dbVerdict = "NEEDS_WORK"
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReviewSubmitResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`UPDATE review_results SET is_latest = FALSE WHERE task_id = $1 AND reviewer_model = $2`,
		taskID, reviewer,
	); err != nil {
		return ReviewSubmitResult{}, fmt.Errorf("mark prior not-latest: %w", err)
	}

	var reviewID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO review_results
		     (task_id, verdict, summary, coverage_score, reviewed_at, reviewer_model, cost_usd)
		 VALUES ($1, $2, $3, 0, NOW()::text, $4, 0)
		 RETURNING id`,
		taskID, dbVerdict, summary, reviewer,
	).Scan(&reviewID); err != nil {
		return ReviewSubmitResult{}, fmt.Errorf("insert review: %w", err)
	}

	auditField := "review"
	auditNew := fmt.Sprintf("%s:%s", reviewer, verdict)
	if isAggregate {
		auditField = "aggregate_review_verdict"
		auditNew = verdict
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, $3, '', $4, 'review-submit', NOW()::text)`,
		taskID, caller, auditField, auditNew,
	); err != nil {
		return ReviewSubmitResult{}, fmt.Errorf("audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ReviewSubmitResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	return ReviewSubmitResult{
		TaskID:      taskID,
		Caller:      caller,
		Reviewer:    reviewer,
		Verdict:     verdict,
		IsAggregate: isAggregate,
		ReviewID:    reviewID,
		Summary:     summary,
	}, nil
}

// ---------------------------------------------------------------------------
// verify / postmortem / revision — infra_fix + creative workflow helpers
// ---------------------------------------------------------------------------

// VerifyResult is what VerifyTask returns to the handler. The task field is
// refetched post-commit so updated_at, reporter_verified, and (in the --failed
// path) status reflect the DB row rather than the pre-mutation copy.
type VerifyResult struct {
	Task     Task                 `json:"task"`
	Mode     string               `json:"mode"` // "positive" | "failed"
	Reason   string               `json:"reason,omitempty"`
	Failures []AdvanceGateFailure `json:"failures,omitempty"`
}

// VerifyTask records a reporter verification for an infra_fix task.
//
//   - Positive path (failed == ""): UPDATE reporter_verified=1 +
//     reporter_verified_at=NOW() + audit row (field=reporter_verified).
//   - Failed path (failed != ""): UPDATE status='INVESTIGATING' +
//     reporter_verified=0 + reporter_verified_at=NULL + append VERIFY FAILED
//     note to progress + audit row (field=status, command='verify --failed').
//
// The Python cmd_verify validates the transition to INVESTIGATING via
// validate_transition and pages the task owner on failure. Neither is ported
// yet: the transition check is a follow-up port (verifying a non-infra task
// is a harmless no-op today per Python design), and the pager notification
// belongs to a different concern.
func (s *Store) VerifyTask(ctx context.Context, taskID int, agent, failed string) (VerifyResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return VerifyResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return VerifyResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	mode := "positive"
	if failed != "" {
		mode = "failed"
		// Failed path: send back to INVESTIGATING, clear reporter_verified,
		// append the reason to progress. Progress accumulates newline-separated.
		note := fmt.Sprintf("VERIFY FAILED: %s", failed)
		from := strings.ToUpper(strings.TrimSpace(task.Status))
		if _, err := tx.Exec(ctx,
			`UPDATE tasks
			   SET status = 'INVESTIGATING',
			       reporter_verified = 0,
			       reporter_verified_at = NULL,
			       progress = TRIM(BOTH E'\n' FROM COALESCE(progress, '') || E'\n' || $1),
			       updated_at = NOW()::text
			 WHERE id = $2`,
			note, taskID,
		); err != nil {
			return VerifyResult{}, fmt.Errorf("update: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'status', $3, 'INVESTIGATING', 'verify --failed', NOW()::text)`,
			taskID, agent, from,
		); err != nil {
			return VerifyResult{}, fmt.Errorf("audit: %w", err)
		}
	} else {
		// Positive path: set reporter_verified=1 + timestamp.
		if _, err := tx.Exec(ctx,
			`UPDATE tasks
			   SET reporter_verified = 1,
			       reporter_verified_at = NOW()::text,
			       updated_at = NOW()::text
			 WHERE id = $1`,
			taskID,
		); err != nil {
			return VerifyResult{}, fmt.Errorf("update: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'reporter_verified', 'False', 'True', 'verify', NOW()::text)`,
			taskID, agent,
		); err != nil {
			return VerifyResult{}, fmt.Errorf("audit: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return VerifyResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
	}
	return VerifyResult{Task: updated, Mode: mode, Reason: failed}, nil
}

// PostmortemResult mirrors VerifyResult shape without the Mode field.
type PostmortemResult struct {
	Task     Task                 `json:"task"`
	Path     string               `json:"path"`
	Failures []AdvanceGateFailure `json:"failures,omitempty"`
}

// SetPostmortem records the postmortem doc path for an infra_fix task. The
// server has no local FS view of the spec_dir; existence-of-file warnings are
// a client-side concern (Python cmd_postmortem prints them to stderr).
func (s *Store) SetPostmortem(ctx context.Context, taskID int, agent, path string) (PostmortemResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return PostmortemResult{}, err
	}
	oldPath := ""
	// The Task struct doesn't expose postmortem_path today (not in read
	// endpoints yet), so audit records old_value = "" for now. Audit still
	// captures the new value which is what downstream consumers need.

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PostmortemResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET postmortem_path = $1, updated_at = NOW()::text WHERE id = $2`,
		path, taskID,
	); err != nil {
		return PostmortemResult{}, fmt.Errorf("update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'postmortem_path', $3, $4, 'postmortem', NOW()::text)`,
		taskID, agent, oldPath, path,
	); err != nil {
		return PostmortemResult{}, fmt.Errorf("audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PostmortemResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
	}
	return PostmortemResult{Task: updated, Path: path}, nil
}

// RevisionResult is what RequestRevision returns to the handler.
type RevisionResult struct {
	Task       Task                 `json:"task"`
	FromStatus string               `json:"from_status"`
	ToStatus   string               `json:"to_status"`
	Reason     string               `json:"reason"`
	Failures   []AdvanceGateFailure `json:"failures,omitempty"`
}

// RequestRevision moves an AWAITING_APPROVAL task sideways to REVISION with
// the reason recorded in progress. REVISION is a sideways status only
// reachable via this endpoint (see Python cmd_revision docstring).
//
// The server does NOT validate that the task's workflow declares REVISION as
// an allowed target (Python reads TRANSITIONS[AWAITING_APPROVAL] for that).
// For MVP the only source check is fromStatus == 'AWAITING_APPROVAL'; a
// workflow-aware follow-up will tighten this.
func (s *Store) RequestRevision(ctx context.Context, taskID int, agent, reason string) (RevisionResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return RevisionResult{}, err
	}
	from := strings.ToUpper(strings.TrimSpace(task.Status))
	if from != "AWAITING_APPROVAL" {
		return RevisionResult{
			Task: task, FromStatus: from, ToStatus: "REVISION", Reason: reason,
			Failures: []AdvanceGateFailure{{
				Check:  "TRANSITION",
				Detail: fmt.Sprintf("revision is only available from AWAITING_APPROVAL (current: %s)", from),
			}},
		}, nil
	}

	note := fmt.Sprintf("REVISION requested: %s", reason)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RevisionResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`UPDATE tasks
		   SET status = 'REVISION',
		       progress = TRIM(BOTH E'\n' FROM COALESCE(progress, '') || E'\n' || $1),
		       updated_at = NOW()::text
		 WHERE id = $2`,
		note, taskID,
	); err != nil {
		return RevisionResult{}, fmt.Errorf("update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'REVISION', 'revision', NOW()::text)`,
		taskID, agent, from,
	); err != nil {
		return RevisionResult{}, fmt.Errorf("audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RevisionResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = "REVISION"
	}
	return RevisionResult{Task: updated, FromStatus: from, ToStatus: "REVISION", Reason: reason}, nil
}

// ---------------------------------------------------------------------------
// edges — task_edges rows for a task (both directions)
// ---------------------------------------------------------------------------

// TaskEdge is one row of the task_edges table. Direction fields are omitted:
// callers know the task they queried and can compute in/out from
// FromTaskID vs ToTaskID.
type TaskEdge struct {
	FromTaskID int    `json:"from_task_id"`
	ToTaskID   int    `json:"to_task_id"`
	EdgeType   string `json:"edge_type"`
}

// TaskEdges returns every task_edges row incident to taskID, in either
// direction, ordered by (from_task_id, to_task_id) for stable output. Empty
// slice when there are no edges — no 404 (a task with zero edges is a valid
// state, and this endpoint is meant to be polled).
func (s *Store) TaskEdges(ctx context.Context, taskID int) ([]TaskEdge, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT from_task_id, to_task_id, edge_type
		   FROM task_edges
		  WHERE from_task_id = $1 OR to_task_id = $1
		  ORDER BY from_task_id, to_task_id`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("query task_edges: %w", err)
	}
	defer rows.Close()
	out := []TaskEdge{}
	for rows.Next() {
		var e TaskEdge
		if err := rows.Scan(&e.FromTaskID, &e.ToTaskID, &e.EdgeType); err != nil {
			return nil, fmt.Errorf("scan task_edges: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// anomalies — thin subset of Python cmd_anomalies (5 of 6 checks)
// ---------------------------------------------------------------------------

// AnomaliesResult is the JSON shape returned by GET /anomalies. Every list
// holds human-readable strings identical (line-for-line) to what Python's
// cmd_anomalies prints, so downstream tooling that greps the /exec output
// keeps working when it switches to native.
type AnomaliesResult struct {
	BareBlocker []string `json:"bare_blocker"`
	NoTaskPlan  []string `json:"no_taskplan"`
	Stale       []string `json:"stale"`
	OrphanDep   []string `json:"orphan_dep"`
	NoDoneWhen  []string `json:"no_donewhen"`
}

// closedStatuses mirrors Python's _CLOSED_STATUSES set — tasks in these are
// skipped by every anomaly check.
var closedStatuses = map[string]bool{
	"DONE": true, "CANCELLED": true, "SUPERSEDED": true, "WONT-DO": true, "MERGED": true,
}

// Anomalies inspects the whole tasks table and returns a bucketed list of
// human-readable anomaly strings, matching Python cmd_anomalies exactly on
// five of the six anomaly types. The sixth (missing-context-map) is skipped
// because it needs a yaml file the server doesn't ship. Callers that want
// the missing-context-map check must stay on /exec.
//
// Costs one SELECT of the whole tasks table (id, status, mode, workflow,
// task_plan, done_when, blocked_by, created_at). Everything else runs in
// memory. Fine for the ≈1300-row prod backlog; would need indexing if we
// hit five figures.
func (s *Store) Anomalies(ctx context.Context) (AnomaliesResult, error) {
	res := AnomaliesResult{
		BareBlocker: []string{},
		NoTaskPlan:  []string{},
		Stale:       []string{},
		OrphanDep:   []string{},
		NoDoneWhen:  []string{},
	}

	type row struct {
		id        int
		status    string
		mode      string
		workflow  string
		taskPlan  string
		doneWhen  []int // len only used
		blockedBy []int
		createdAt string
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, status, mode, COALESCE(workflow, ''), COALESCE(task_plan, ''),
		        COALESCE(done_when, '[]'), COALESCE(blocked_by, '[]'),
		        COALESCE(created_at, '')
		   FROM tasks`,
	)
	if err != nil {
		return AnomaliesResult{}, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()

	all := []row{}
	byID := map[int]row{}
	allIDs := map[int]bool{}
	for rows.Next() {
		var r row
		var dwJSON, bbJSON string
		if err := rows.Scan(&r.id, &r.status, &r.mode, &r.workflow, &r.taskPlan, &dwJSON, &bbJSON, &r.createdAt); err != nil {
			return AnomaliesResult{}, fmt.Errorf("scan tasks: %w", err)
		}
		r.doneWhen = parseIntArray(dwJSON) // placeholder — we only use len
		if len(r.doneWhen) == 0 {
			// done_when is []string; parseIntArray returns [] for anything
			// that isn't a plain-int list, so we can't rely on it for
			// existence. Re-check by parsing as string list.
			var strs []string
			_ = json.Unmarshal([]byte(dwJSON), &strs)
			for range strs {
				r.doneWhen = append(r.doneWhen, 0) // marker so len > 0
			}
		}
		r.blockedBy = parseIntArray(bbJSON)
		all = append(all, r)
		byID[r.id] = r
		allIDs[r.id] = true
	}
	if err := rows.Err(); err != nil {
		return AnomaliesResult{}, err
	}

	// Parse cutoff date once. Everything after this stays deterministic.
	nowStr := ""
	if err := s.pool.QueryRow(ctx, `SELECT NOW()::date::text`).Scan(&nowStr); err != nil {
		return AnomaliesResult{}, fmt.Errorf("now(): %w", err)
	}
	staleWorkflows := map[string]bool{"marketing": true, "research_task": true}

	for _, t := range all {
		if closedStatuses[strings.ToUpper(t.status)] {
			continue
		}

		// BARE_BLOCKER: blocked_by id that isn't a real task.
		for _, bid := range t.blockedBy {
			if !allIDs[bid] {
				res.BareBlocker = append(res.BareBlocker, fmt.Sprintf("#%d blocked_by #%d -- task #%d does not exist", t.id, bid, bid))
			}
		}

		// NO_TASKPLAN: non-early-status Code/Think tasks lacking task_plan
		// (skip marketing / research_task — they use brief.md).
		if t.status != "BACKLOG" && t.status != "TODO" && t.status != "PLANNING" {
			if (t.mode == "Code" || t.mode == "Think") && t.taskPlan == "" && !staleWorkflows[t.workflow] {
				res.NoTaskPlan = append(res.NoTaskPlan, fmt.Sprintf("#%d is %s task in %s without task_plan", t.id, t.mode, t.status))
			}
		}

		// STALE: TODO/IN_PROGRESS since created_at, >14 days.
		if (t.status == "IN_PROGRESS" || t.status == "TODO") && t.createdAt != "" {
			days := daysBetween(t.createdAt, nowStr)
			if days > 14 {
				res.Stale = append(res.Stale, fmt.Sprintf("#%d %s since %s (%d days)", t.id, t.status, t.createdAt, days))
			}
		}

		// ORPHAN_DEP: blocked_by that is already DONE.
		for _, bid := range t.blockedBy {
			if b, ok := byID[bid]; ok && strings.ToUpper(b.status) == "DONE" {
				res.OrphanDep = append(res.OrphanDep, fmt.Sprintf("#%d blocked_by #%d, but #%d is DONE", t.id, bid, bid))
			}
		}

		// NO_DONEWHEN: non-early-status task with empty done_when.
		if t.status != "BACKLOG" && t.status != "TODO" && t.status != "PLANNING" {
			if len(t.doneWhen) == 0 {
				res.NoDoneWhen = append(res.NoDoneWhen, fmt.Sprintf("#%d status %s but done_when is empty", t.id, t.status))
			}
		}
	}
	return res, nil
}

// daysBetween returns the whole-day delta between two YYYY-MM-DD strings.
// Falls back to 0 on parse error (matches Python's silent skip on bad dates).
func daysBetween(fromDate, toDate string) int {
	if len(fromDate) < 10 || len(toDate) < 10 {
		return 0
	}
	// Both prefixes are YYYY-MM-DD. Compare as time.Time; fmt.Sscanf would
	// swallow the timezone bits which are harmless here.
	const layout = "2006-01-02"
	f, err := time.Parse(layout, fromDate[:10])
	if err != nil {
		return 0
	}
	t, err := time.Parse(layout, toDate[:10])
	if err != nil {
		return 0
	}
	return int(t.Sub(f).Hours() / 24)
}

// ---------------------------------------------------------------------------
// analytics — thin subset of Python cmd_analytics (velocity + by-agent counts)
// ---------------------------------------------------------------------------

// AnalyticsResult is the JSON shape returned by GET /analytics.
type AnalyticsResult struct {
	Velocity     AnalyticsVelocity      `json:"velocity"`
	ByAgent      []AnalyticsAgentRow    `json:"by_agent"`
	StatusCounts map[string]int         `json:"status_counts"`
}

type AnalyticsVelocity struct {
	PeriodDays    int     `json:"period_days"`
	TotalClosed   int     `json:"total_closed"`
	TasksPerWeek  float64 `json:"tasks_per_week"`
}

type AnalyticsAgentRow struct {
	Owner    string `json:"owner"`
	Total    int    `json:"total"`
	Done     int    `json:"done"`
	Active   int    `json:"active"`   // TODO/PLANNING/READY/IN_PROGRESS/REOPENED
	Blocked  int    `json:"blocked"`  // BLOCKED / IN_REVIEW / AWAITING_APPROVAL
}

// Analytics returns a JSON-only slice of the Python cmd_analytics output:
// velocity (DONE transitions in the last period_days), by-agent counts, and
// per-status totals. Bottlenecks, time-in-status, and cost sections stay on
// the /exec path — those are heavier queries with formatting overhead the
// native endpoint intentionally does not try to match.
//
// periodDays defaults to 28 (cmd_analytics default). agent, when non-empty,
// restricts the by-agent slice to that one row.
func (s *Store) Analytics(ctx context.Context, agent string, periodDays int) (AnalyticsResult, error) {
	if periodDays <= 0 {
		periodDays = 28
	}
	res := AnalyticsResult{
		Velocity:     AnalyticsVelocity{PeriodDays: periodDays},
		ByAgent:      []AnalyticsAgentRow{},
		StatusCounts: map[string]int{},
	}

	// Velocity: count DONE transitions in the window.
	var closed int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM audit_trail
		  WHERE field_changed = 'status' AND new_value = 'DONE'
		    AND timestamp >= (NOW() - ($1 || ' days')::interval)::text`,
		fmt.Sprintf("%d", periodDays),
	).Scan(&closed); err != nil {
		return AnalyticsResult{}, fmt.Errorf("velocity: %w", err)
	}
	res.Velocity.TotalClosed = closed
	if periodDays > 0 {
		res.Velocity.TasksPerWeek = round1(float64(closed) * 7.0 / float64(periodDays))
	}

	// Per-status counts. Reuses the same categorisation as /status but keeps
	// them here so a single call yields everything an operator needs.
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err != nil {
		return AnalyticsResult{}, fmt.Errorf("status_counts: %w", err)
	}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return AnalyticsResult{}, err
		}
		res.StatusCounts[status] = n
	}
	rows.Close()

	// By-agent breakdown. Sum status buckets in a single scan so the query
	// stays O(rows) rather than O(agents * statuses).
	args := []any{}
	where := ""
	if agent != "" {
		args = append(args, strings.ToLower(agent))
		where = "WHERE lower(owner) = $1"
	}
	q := fmt.Sprintf(`SELECT owner, status, COUNT(*) FROM tasks %s GROUP BY owner, status`, where)
	aRows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return AnalyticsResult{}, fmt.Errorf("by_agent: %w", err)
	}
	byOwner := map[string]*AnalyticsAgentRow{}
	for aRows.Next() {
		var owner, status string
		var n int
		if err := aRows.Scan(&owner, &status, &n); err != nil {
			aRows.Close()
			return AnalyticsResult{}, err
		}
		row, ok := byOwner[owner]
		if !ok {
			row = &AnalyticsAgentRow{Owner: owner}
			byOwner[owner] = row
		}
		row.Total += n
		st := strings.ToUpper(status)
		switch st {
		case "DONE":
			row.Done += n
		case "TODO", "PLANNING", "READY", "IN_PROGRESS", "REOPENED":
			row.Active += n
		case "BLOCKED", "IN_REVIEW", "AWAITING_APPROVAL":
			row.Blocked += n
		}
	}
	aRows.Close()
	for _, r := range byOwner {
		res.ByAgent = append(res.ByAgent, *r)
	}
	// Deterministic order: by owner ASC so the JSON is diffable across calls.
	sort.Slice(res.ByAgent, func(i, j int) bool { return res.ByAgent[i].Owner < res.ByAgent[j].Owner })

	return res, nil
}

// ---------------------------------------------------------------------------
// search — simplified subset of Python cmd_search
// ---------------------------------------------------------------------------

// searchSortFields is the allow-list for the ?sort= query param. Anything
// outside falls back to effective_score (cmd_search default). Restricting
// the set here is deliberate: the frontend never asks to sort by JSON
// columns (blocked_by, done_when) and letting an arbitrary column name
// through would open an SQL-injection edge case on the ORDER BY clause.
var searchSortFields = map[string]bool{
	"effective_score": true,
	"updated_at":      true,
	"created_at":      true,
	"id":              true,
	"status":          true,
	"owner":           true,
	"title":           true,
}

// SearchTasks is the native counterpart to Python cmd_search's happy path:
// owner + status + free-text (matched against title/why/note) with an ORDER
// BY + LIMIT. Negation filters, positional word AND-combos, and
// --client/--project soft-refs stay on /exec — the CLI dispatcher only picks
// the native path when the incoming request fits inside this shape.
//
// text is matched with ILIKE '%text%' against title/why/note (case-
// insensitive, matches Python's LIKE + upper()/lower() call sites).
func (s *Store) SearchTasks(ctx context.Context, owner, status, text, sort string, limit int) ([]Task, error) {
	clauses := []string{}
	args := []any{}
	if owner != "" {
		args = append(args, strings.ToLower(owner))
		clauses = append(clauses, fmt.Sprintf("lower(owner) = $%d", len(args)))
	}
	if status != "" {
		args = append(args, strings.ToUpper(status))
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	if text != "" {
		args = append(args, "%"+text+"%")
		p := len(args)
		clauses = append(clauses, fmt.Sprintf("(title ILIKE $%d OR why ILIKE $%d OR note ILIKE $%d)", p, p, p))
	}
	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	sort = strings.ToLower(strings.TrimSpace(sort))
	if !searchSortFields[sort] {
		sort = "effective_score"
	}
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	args = append(args, limit)
	q := fmt.Sprintf(
		"SELECT %s FROM tasks %s ORDER BY %s DESC NULLS LAST LIMIT $%d",
		taskColumns, where, sort, len(args),
	)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query search: %w", err)
	}
	defer rows.Close()
	out := []Task{}
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan search: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// history — audit_trail rows for a task
// ---------------------------------------------------------------------------

// AuditRow is one row of the audit_trail exposed by GET /task/{id}/history.
type AuditRow struct {
	ID           int64  `json:"id"`
	Agent        string `json:"agent"`
	Timestamp    string `json:"timestamp"`
	FieldChanged string `json:"field_changed"`
	OldValue     string `json:"old_value"`
	NewValue     string `json:"new_value"`
	Reason       string `json:"reason"`
	Command      string `json:"command"`
}

// TaskHistory returns audit_trail rows for taskID, oldest first (parity with
// Python cmd_history's `ORDER BY timestamp ASC`). Default limit is 50 to
// match cmd_history's default; pass 0 to disable the LIMIT clause.
//
// Read is idempotent — a non-existent task returns an empty slice instead of
// 404. This matches how audit-trail-style feeds behave (append-only, no
// consumer needs presence-check).
func (s *Store) TaskHistory(ctx context.Context, taskID, limit int) ([]AuditRow, error) {
	if limit < 0 {
		limit = 0
	}
	if limit == 0 {
		limit = 50 // Python cmd_history default
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, agent, timestamp, field_changed,
		        COALESCE(old_value, ''), COALESCE(new_value, ''),
		        COALESCE(reason, ''), COALESCE(command, '')
		   FROM audit_trail
		  WHERE task_id = $1
		  ORDER BY timestamp ASC
		  LIMIT $2`,
		taskID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query audit_trail: %w", err)
	}
	defer rows.Close()
	out := []AuditRow{}
	for rows.Next() {
		var r AuditRow
		if err := rows.Scan(&r.ID, &r.Agent, &r.Timestamp, &r.FieldChanged, &r.OldValue, &r.NewValue, &r.Reason, &r.Command); err != nil {
			return nil, fmt.Errorf("scan audit_trail: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// knowledge — ADR-style entries stored in knowledge_entries
// ---------------------------------------------------------------------------

// KnowledgeResult is what AddKnowledge returns to the handler.
type KnowledgeResult struct {
	ID           int64  `json:"id"`
	TaskID       *int   `json:"task_id"`
	Context      string `json:"context"`
	Decision     string `json:"decision"`
	Consequences string `json:"consequences"`
	Source       string `json:"source"`
	CreatedAt    string `json:"created_at"`
}

// AddKnowledge inserts an ADR-style row into knowledge_entries. task_id is
// optional (schema allows NULL for orphan ADRs). Handler-level check enforces
// that at least one of {context, decision, consequences} is non-empty to
// avoid silent empty-row spam.
//
// No CLI wire yet — the existing `backlogist knowledge` subcommand covers
// search only. Automatic entries are saved by the reviewer pipeline via
// backlogist/core/knowledge.py::save_knowledge (unchanged, direct PG).
func (s *Store) AddKnowledge(ctx context.Context, taskID *int, context_, decision, consequences, source string) (KnowledgeResult, error) {
	var (
		id        int64
		createdAt string
	)
	err := s.pool.QueryRow(ctx,
		`INSERT INTO knowledge_entries (task_id, context, decision, consequences, source, created_at)
		 VALUES ($1, $2, $3, $4, $5, NOW()::text)
		 RETURNING id, created_at`,
		taskID, context_, decision, consequences, source,
	).Scan(&id, &createdAt)
	if err != nil {
		return KnowledgeResult{}, fmt.Errorf("insert knowledge: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	return KnowledgeResult{
		ID:           id,
		TaskID:       taskID,
		Context:      context_,
		Decision:     decision,
		Consequences: consequences,
		Source:       source,
		CreatedAt:    createdAt,
	}, nil
}

// ---------------------------------------------------------------------------
// freeze-update — intent versioning + history archival
// ---------------------------------------------------------------------------

// FreezeUpdateResult is what FreezeUpdate returns to the handler.
type FreezeUpdateResult struct {
	TaskID      int                  `json:"task_id"`
	FromVersion int                  `json:"from_version"` // 0 when initialising
	ToVersion   int                  `json:"to_version"`
	Initialized bool                 `json:"initialized"` // true when frozen_intent was NULL
	IntentText  string               `json:"intent_text"`
	Failures    []AdvanceGateFailure `json:"failures,omitempty"`
}

// FreezeUpdate re-freezes a task's frozen_intent from the current note+why
// with version history (parity with backlogist/core/intent_gate.py::freeze_update).
//
//   - If task.frozen_intent IS NULL: sets frozen_intent = new_intent,
//     intent_source='operator', intent_version=1. No history row.
//   - Otherwise: archives the current (version, intent_text, reason, author)
//     to tasks_intent_history, then updates frozen_intent = new_intent and
//     bumps intent_version.
//
// new_intent is re-read from the CURRENT task.note + task.why — caller must
// update those first via `backlogist #N update ...`. Empty new_intent → 422
// EMPTY_INTENT (parity with Python's BacklogistError).
//
// Author is hardcoded 'operator' (Python parity). Native does not thread
// AX_AGENT into the history row; that's a small semantic drift worth a
// follow-up if operators want per-agent attribution.
func (s *Store) FreezeUpdate(ctx context.Context, taskID int, agent, reason string) (FreezeUpdateResult, error) {
	// Read the columns we need directly (Task struct doesn't expose the
	// intent-* fields yet; adding them to the read endpoints is a separate change).
	var (
		note          string
		why           string
		currentFrozen *string
		currentVer    int
	)
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(note, ''), COALESCE(why, ''), frozen_intent, intent_version FROM tasks WHERE id = $1`,
		taskID,
	).Scan(&note, &why, &currentFrozen, &currentVer)
	if err != nil {
		return FreezeUpdateResult{}, err
	}
	newIntent := strings.TrimSpace(strings.TrimSpace(note) + "\n" + strings.TrimSpace(why))
	if newIntent == "" {
		return FreezeUpdateResult{
			TaskID: taskID, FromVersion: currentVer, ToVersion: currentVer,
			Failures: []AdvanceGateFailure{{
				Check:  "EMPTY_INTENT",
				Detail: "Cannot freeze: task.note and task.why are both empty. Update them first: backlogist #" + fmt.Sprintf("%d", taskID) + " update note:\"…\"",
			}},
		}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return FreezeUpdateResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	initialized := currentFrozen == nil
	fromVer := currentVer
	toVer := currentVer
	if initialized {
		// Fresh task — set intent + reset version to 1 (the default is already 1
		// but this makes the write explicit and idempotent against schemas where
		// the default may have changed).
		if _, err := tx.Exec(ctx,
			`UPDATE tasks
			   SET frozen_intent = $1,
			       intent_source = 'operator',
			       intent_version = 1,
			       updated_at = NOW()::text
			 WHERE id = $2`,
			newIntent, taskID,
		); err != nil {
			return FreezeUpdateResult{}, fmt.Errorf("update tasks: %w", err)
		}
		fromVer = 0 // Signal "was NULL"
		toVer = 1
	} else {
		// Archive the current version, then bump.
		if _, err := tx.Exec(ctx,
			`INSERT INTO tasks_intent_history (task_id, version, intent_text, reason, author)
			 VALUES ($1, $2, $3, $4, 'operator')`,
			taskID, currentVer, *currentFrozen, reason,
		); err != nil {
			return FreezeUpdateResult{}, fmt.Errorf("insert history: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE tasks
			   SET frozen_intent = $1,
			       intent_version = intent_version + 1,
			       updated_at = NOW()::text
			 WHERE id = $2`,
			newIntent, taskID,
		); err != nil {
			return FreezeUpdateResult{}, fmt.Errorf("update tasks: %w", err)
		}
		toVer = currentVer + 1
	}

	if err := tx.Commit(ctx); err != nil {
		return FreezeUpdateResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	return FreezeUpdateResult{
		TaskID:      taskID,
		FromVersion: fromVer,
		ToVersion:   toVer,
		Initialized: initialized,
		IntentText:  newIntent,
	}, nil
}

// updatableTextFields is the whitelist for safe UpdateTask targets. Excludes:
//  - status (transition gates + auto-actions must run Python-side for now)
//  - custom_fields (JSON merge semantics; Python has explicit merge logic)
//  - client_id/project_id (UUID normalize + cross-DB validation)
//  - JSON list columns (done_when, references, tags, blocked_by) — MVP scope
//  - review_result (append-only column touched by review-submit path)
var updatableTextFields = map[string]bool{
	"title":          true,
	"why":            true,
	"mode":           true,
	"note":           true,
	"business_value": true,
	"task_plan":      true,
	"spec":           true,
	"owner":          true,
	"parent_task_id": true, // integer but stored as int col — Python does str->int; handle at SQL level
}

// UpdateResult mirrors AdvanceResult but returns the updated task and per-field diffs.
type UpdateResult struct {
	Task     Task                 `json:"task"`
	Changes  map[string][2]string `json:"changes"` // field → [old, new]
	Failures []AdvanceGateFailure `json:"failures,omitempty"`
}

// UpdateTask applies a whitelist of safe text-field updates atomically with audit
// rows per changed field. Complex updates (status transitions, custom_fields
// JSON merge, JSON list columns) intentionally remain on the /exec path.
func (s *Store) UpdateTask(ctx context.Context, taskID int, agent string, updates map[string]string) (UpdateResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return UpdateResult{}, err
	}
	if len(updates) == 0 {
		return UpdateResult{Task: task, Changes: map[string][2]string{}}, nil
	}
	// Reject unknown / non-whitelisted fields all at once so the client sees the full list.
	var rejected []string
	for k := range updates {
		if !updatableTextFields[k] {
			rejected = append(rejected, k)
		}
	}
	if len(rejected) > 0 {
		return UpdateResult{
			Task: task,
			Failures: []AdvanceGateFailure{{
				Check:  "FIELDS",
				Detail: fmt.Sprintf("fields not supported by native update: %s. Use /exec fallback (backlogist #%d update %s:…) or extend server whitelist.", strings.Join(rejected, ", "), taskID, rejected[0]),
			}},
		}, nil
	}
	// Diff first — skip DB round trip if all no-ops
	changes := map[string][2]string{}
	oldByField := map[string]string{}
	for field, newV := range updates {
		oldV := currentTextField(task, field)
		oldByField[field] = oldV
		if oldV != newV {
			changes[field] = [2]string{oldV, newV}
		}
	}
	if len(changes) == 0 {
		return UpdateResult{Task: task, Changes: changes}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Build one UPDATE with all changed fields. Order args stably (sorted keys)
	// so the parameterisation is deterministic (easier to log/debug).
	fields := make([]string, 0, len(changes))
	for f := range changes {
		fields = append(fields, f)
	}
	sort.Strings(fields)

	setClauses := []string{}
	args := []any{}
	for i, f := range fields {
		val := changes[f][1]
		if f == "parent_task_id" {
			// Store as INTEGER (NULL when empty). Skip zero-length to allow clearing.
			if val == "" {
				setClauses = append(setClauses, fmt.Sprintf("parent_task_id = NULL"))
				continue
			}
			args = append(args, val)
			setClauses = append(setClauses, fmt.Sprintf("parent_task_id = $%d::integer", len(args)))
			_ = i
			continue
		}
		args = append(args, val)
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", f, len(args)))
	}
	setClauses = append(setClauses, "updated_at = NOW()::text")
	args = append(args, taskID)
	q := fmt.Sprintf("UPDATE tasks SET %s WHERE id = $%d", strings.Join(setClauses, ", "), len(args))
	if _, err := tx.Exec(ctx, q, args...); err != nil {
		return UpdateResult{}, fmt.Errorf("update: %w", err)
	}
	for _, f := range fields {
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, $3, $4, $5, 'update', NOW()::text)`,
			taskID, agent, f, changes[f][0], changes[f][1],
		); err != nil {
			return UpdateResult{}, fmt.Errorf("audit %s: %w", f, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return UpdateResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	// Refresh task from DB to reflect all changes cleanly (including auto columns).
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		// Non-fatal: return the stale-ish task with mutations applied in-memory.
		updated = task
		for f, diff := range changes {
			applyTextField(&updated, f, diff[1])
		}
	}
	return UpdateResult{Task: updated, Changes: changes}, nil
}

// currentTextField returns the current value of a whitelisted text field.
func currentTextField(t Task, field string) string {
	switch field {
	case "title":
		return t.Title
	case "why":
		return t.Why
	case "mode":
		return t.Mode
	case "note":
		return t.Note
	case "business_value":
		return t.BusinessValue
	case "task_plan":
		return t.TaskPlan
	case "spec":
		return t.Spec
	case "owner":
		return t.Owner
	case "parent_task_id":
		if t.ParentTaskID == nil {
			return ""
		}
		return fmt.Sprintf("%d", *t.ParentTaskID)
	}
	return ""
}

// applyTextField mutates the Task in-memory (fallback when re-fetching failed).
func applyTextField(t *Task, field, v string) {
	switch field {
	case "title":
		t.Title = v
	case "why":
		t.Why = v
	case "mode":
		t.Mode = v
	case "note":
		t.Note = v
	case "business_value":
		t.BusinessValue = v
	case "task_plan":
		t.TaskPlan = v
	case "spec":
		t.Spec = v
	case "owner":
		t.Owner = v
	}
}

// releaseableStatuses — from IN_PROGRESS or REOPENED back to READY (or PLANNING
// if that's what the operator wants; MVP: always to READY).
var releaseableStatuses = map[string]bool{
	"IN_PROGRESS": true,
	"REOPENED":    true,
}

// ReleaseTask sets status=READY + audit. Doesn't clear owner (keeps ownership
// so history is preserved; the Python cmd_release also leaves owner alone).
// No time-entry close: side effect deferred to a future commit.
func (s *Store) ReleaseTask(ctx context.Context, taskID int, agent string) (AdvanceResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return AdvanceResult{}, err
	}
	from := strings.ToUpper(strings.TrimSpace(task.Status))
	if !releaseableStatuses[from] {
		return AdvanceResult{
			Task: task, FromStatus: from, ToStatus: "READY",
			Failures: []AdvanceGateFailure{{
				Check:  "TRANSITION",
				Detail: fmt.Sprintf("cannot release task in status %s (only IN_PROGRESS/REOPENED)", from),
			}},
		}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'READY', updated_at = NOW()::text WHERE id = $1`,
		taskID,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'READY', 'release', NOW()::text)`,
		taskID, agent, from,
	); err != nil {
		return AdvanceResult{}, fmt.Errorf("audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	updated, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		updated = task
		updated.Status = "READY"
	}
	return AdvanceResult{Task: updated, FromStatus: from, ToStatus: "READY"}, nil
}

// ---------------------------------------------------------------------------
// Merge: absorb task B into task A
// ---------------------------------------------------------------------------

// mergeTerminalStatuses: statuses from which neither side of a merge is allowed.
// Mirrors backlogist/core/commands.py::cmd_merge — CANCELLED and SUPERSEDED are
// the only rejections; DONE is permitted (Python parity, see test_merge.py).
var mergeTerminalStatuses = map[string]bool{
	"CANCELLED":  true,
	"SUPERSEDED": true,
}

// MergeResult mirrors the shape returned by cmd_merge in Python: the union'd
// A, the fields that actually changed, the redirected dependents, and B's
// terminal status. Failures[] used for 422 gate failures (terminal status,
// self-merge). dry_run flag echoed back for client display.
type MergeResult struct {
	TaskA             Task                 `json:"task_a"`
	AbsorbedID        int                  `json:"absorbed_id"`
	FieldsUpdated     []string             `json:"fields_updated"`
	RedirectedDepIDs  []int                `json:"redirected_dep_ids"`
	BStatus           string               `json:"b_status"`
	DryRun            bool                 `json:"dry_run"`
	Failures          []AdvanceGateFailure `json:"failures,omitempty"`
}

// MergeTasks absorbs task B (absorbedID) into task A (absorberID):
//   - union of list fields (blocked_by/consumers/done_when/references/tags) onto A
//   - done_when items from B get "(from #B) " prefix (provenance preservation)
//   - note appended if B has one
//   - dependents of B (T.blocked_by contains B) get B replaced with A
//   - B.status = SUPERSEDED, B.note = "Merged into #A"
//   - audit_trail entries per changed field + B status change
//
// All writes in one transaction. dryRun computes the diff without persisting.
//
// Consumers and tags are TEXT-json columns not exposed on the Task struct; we
// touch them via raw SQL SELECT/UPDATE inside the tx to avoid a struct churn
// PR that would ripple through every read endpoint.
//
// Not ported from Python cmd_merge:
//   - export_yaml() — native callers work through HTTP; local yaml is a Python
//     CLI convenience not needed here.
//   - stdout progress lines ("  blocked_by: added [...]", etc.) — the JSON
//     result captures the same information in structured form.
func (s *Store) MergeTasks(ctx context.Context, absorberID, absorbedID int, agent string, dryRun bool) (MergeResult, error) {
	if absorberID == absorbedID {
		return MergeResult{
			AbsorbedID: absorbedID,
			Failures: []AdvanceGateFailure{{
				Check:  "SAME_TASK",
				Detail: fmt.Sprintf("cannot merge task into itself (#%d)", absorberID),
			}},
		}, nil
	}
	taskA, _, err := s.GetTask(ctx, absorberID)
	if err != nil {
		return MergeResult{}, err
	}
	taskB, _, err := s.GetTask(ctx, absorbedID)
	if err != nil {
		return MergeResult{}, err
	}
	fromA := strings.ToUpper(strings.TrimSpace(taskA.Status))
	fromB := strings.ToUpper(strings.TrimSpace(taskB.Status))
	if mergeTerminalStatuses[fromA] {
		return MergeResult{
			TaskA: taskA, AbsorbedID: absorbedID, DryRun: dryRun,
			Failures: []AdvanceGateFailure{{
				Check:  "TARGET_TERMINAL",
				Detail: fmt.Sprintf("cannot merge into #%d: status is %s", absorberID, fromA),
			}},
		}, nil
	}
	if mergeTerminalStatuses[fromB] {
		return MergeResult{
			TaskA: taskA, AbsorbedID: absorbedID, DryRun: dryRun,
			Failures: []AdvanceGateFailure{{
				Check:  "ABSORBED_TERMINAL",
				Detail: fmt.Sprintf("task #%d is already %s, cannot merge", absorbedID, fromB),
			}},
		}, nil
	}

	// Fetch consumers + tags via raw SQL (not on the Task struct).
	var aConsumersJSON, aTagsJSON, bConsumersJSON, bTagsJSON string
	if err := s.pool.QueryRow(ctx,
		`SELECT consumers, tags FROM tasks WHERE id = $1`, absorberID,
	).Scan(&aConsumersJSON, &aTagsJSON); err != nil {
		return MergeResult{}, fmt.Errorf("load A consumers/tags: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT consumers, tags FROM tasks WHERE id = $1`, absorbedID,
	).Scan(&bConsumersJSON, &bTagsJSON); err != nil {
		return MergeResult{}, fmt.Errorf("load B consumers/tags: %w", err)
	}
	aConsumers := parseStringArray(aConsumersJSON)
	bConsumers := parseStringArray(bConsumersJSON)
	aTags := parseStringArray(aTagsJSON)
	bTags := parseStringArray(bTagsJSON)

	// --- Compute unions ---
	changedFields := []string{}
	// blocked_by: sorted set union minus A's own id (no self-block)
	newBlockedBy := mergeIntSetSortedExclude(taskA.BlockedBy, taskB.BlockedBy, absorberID)
	blockedChanged := !intSliceEq(newBlockedBy, taskA.BlockedBy)
	if blockedChanged {
		changedFields = append(changedFields, "blocked_by")
	}
	// consumers: sorted set union
	newConsumers := mergeStringSetSorted(aConsumers, bConsumers)
	consumersChanged := !stringSliceEq(newConsumers, aConsumers)
	if consumersChanged {
		changedFields = append(changedFields, "consumers")
	}
	// done_when: append B items with (from #N) prefix if new
	prefix := fmt.Sprintf("(from #%d) ", absorbedID)
	newDoneWhen := append([]string{}, taskA.DoneWhen...)
	aDoneSet := map[string]bool{}
	for _, v := range taskA.DoneWhen {
		aDoneSet[v] = true
	}
	for _, v := range taskB.DoneWhen {
		tagged := prefix + v
		if !aDoneSet[tagged] {
			newDoneWhen = append(newDoneWhen, tagged)
			aDoneSet[tagged] = true
		}
	}
	doneChanged := len(newDoneWhen) != len(taskA.DoneWhen)
	if doneChanged {
		changedFields = append(changedFields, "done_when")
	}
	// references: preserve A order, append B references not in A
	newReferences := append([]string{}, taskA.References...)
	aRefSet := map[string]bool{}
	for _, v := range taskA.References {
		aRefSet[v] = true
	}
	for _, v := range taskB.References {
		if !aRefSet[v] {
			newReferences = append(newReferences, v)
			aRefSet[v] = true
		}
	}
	refsChanged := len(newReferences) != len(taskA.References)
	if refsChanged {
		changedFields = append(changedFields, "references")
	}
	// tags: sorted set union
	newTags := mergeStringSetSorted(aTags, bTags)
	tagsChanged := !stringSliceEq(newTags, aTags)
	if tagsChanged {
		changedFields = append(changedFields, "tags")
	}
	// note: append B.note to A.note with \n separator if both present
	newNote := taskA.Note
	if taskB.Note != "" {
		sep := ""
		if taskA.Note != "" {
			sep = "\n"
		}
		newNote = taskA.Note + sep + taskB.Note
	}
	noteChanged := newNote != taskA.Note
	if noteChanged {
		changedFields = append(changedFields, "note")
	}

	// --- Find dependents of B (T.blocked_by contains absorbedID) ---
	depRows, err := s.pool.Query(ctx,
		`SELECT id, blocked_by
		   FROM tasks
		  WHERE blocked_by::jsonb @> jsonb_build_array($1::int)
		    AND id != $2`,
		absorbedID, absorbedID,
	)
	if err != nil {
		return MergeResult{}, fmt.Errorf("find dependents: %w", err)
	}
	type depRow struct {
		ID           int
		BlockedByOld []int
		BlockedByNew []int
	}
	var deps []depRow
	for depRows.Next() {
		var id int
		var bbJSON string
		if err := depRows.Scan(&id, &bbJSON); err != nil {
			depRows.Close()
			return MergeResult{}, fmt.Errorf("scan dep: %w", err)
		}
		if id == absorberID {
			continue // A is not redirected into itself; skip
		}
		old := parseIntArray(bbJSON)
		// Replace absorbedID with absorberID, then dedup preserving first-seen order.
		seen := map[int]bool{}
		newBB := []int{}
		for _, v := range old {
			target := v
			if v == absorbedID {
				target = absorberID
			}
			if !seen[target] {
				seen[target] = true
				newBB = append(newBB, target)
			}
		}
		deps = append(deps, depRow{ID: id, BlockedByOld: old, BlockedByNew: newBB})
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return MergeResult{}, fmt.Errorf("iter dependents: %w", err)
	}

	redirected := make([]int, 0, len(deps))
	for _, d := range deps {
		redirected = append(redirected, d.ID)
	}

	if dryRun {
		return MergeResult{
			TaskA: taskA, AbsorbedID: absorbedID,
			FieldsUpdated:    changedFields,
			RedirectedDepIDs: redirected,
			BStatus:          fromB, // unchanged in dry-run
			DryRun:           true,
		}, nil
	}

	// --- Transaction: A UPDATE + N deps UPDATE + B UPDATE + audits ---
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MergeResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// UPDATE A with union'd fields (all in one statement).
	newBlockedByJSON := toJSONArrayInts(newBlockedBy)
	newConsumersJSON := toJSONArrayStrings(newConsumers)
	newDoneWhenJSON := toJSONArrayStrings(newDoneWhen)
	newReferencesJSON := toJSONArrayStrings(newReferences)
	newTagsJSON := toJSONArrayStrings(newTags)
	if _, err := tx.Exec(ctx,
		`UPDATE tasks
		    SET blocked_by = $1,
		        consumers  = $2,
		        done_when  = $3,
		        "references" = $4,
		        tags       = $5,
		        note       = $6,
		        updated_at = NOW()::text
		  WHERE id = $7`,
		newBlockedByJSON, newConsumersJSON, newDoneWhenJSON,
		newReferencesJSON, newTagsJSON, newNote, absorberID,
	); err != nil {
		return MergeResult{}, fmt.Errorf("update A: %w", err)
	}

	// audit_trail per changed field on A. command="merge" for grep-ability.
	for _, field := range changedFields {
		var oldVal, newVal string
		switch field {
		case "blocked_by":
			oldVal = fmt.Sprintf("%v", taskA.BlockedBy)
			newVal = fmt.Sprintf("%v", newBlockedBy)
		case "consumers":
			oldVal = fmt.Sprintf("%v", aConsumers)
			newVal = fmt.Sprintf("%v", newConsumers)
		case "done_when":
			oldVal = fmt.Sprintf("%v", taskA.DoneWhen)
			newVal = fmt.Sprintf("%v", newDoneWhen)
		case "references":
			oldVal = fmt.Sprintf("%v", taskA.References)
			newVal = fmt.Sprintf("%v", newReferences)
		case "tags":
			oldVal = fmt.Sprintf("%v", aTags)
			newVal = fmt.Sprintf("%v", newTags)
		case "note":
			oldVal = taskA.Note
			newVal = newNote
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, $3, $4, $5, 'merge', NOW()::text)`,
			absorberID, agent, field, oldVal, newVal,
		); err != nil {
			return MergeResult{}, fmt.Errorf("audit A %s: %w", field, err)
		}
	}

	// UPDATE each dependent's blocked_by + audit.
	for _, d := range deps {
		newBBJSON := toJSONArrayInts(d.BlockedByNew)
		if _, err := tx.Exec(ctx,
			`UPDATE tasks SET blocked_by = $1, updated_at = NOW()::text WHERE id = $2`,
			newBBJSON, d.ID,
		); err != nil {
			return MergeResult{}, fmt.Errorf("update dep %d: %w", d.ID, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'blocked_by', $3, $4, 'merge', NOW()::text)`,
			d.ID, agent, fmt.Sprintf("%v", d.BlockedByOld), fmt.Sprintf("%v", d.BlockedByNew),
		); err != nil {
			return MergeResult{}, fmt.Errorf("audit dep %d: %w", d.ID, err)
		}
	}

	// UPDATE B → SUPERSEDED + note + audit.
	bNote := fmt.Sprintf("Merged into #%d", absorberID)
	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'SUPERSEDED', note = $1, updated_at = NOW()::text WHERE id = $2`,
		bNote, absorbedID,
	); err != nil {
		return MergeResult{}, fmt.Errorf("update B: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
		 VALUES ($1, $2, 'status', $3, 'SUPERSEDED', $4, NOW()::text)`,
		absorbedID, agent, fromB, fmt.Sprintf("merge into #%d", absorberID),
	); err != nil {
		return MergeResult{}, fmt.Errorf("audit B: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return MergeResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	// Refresh A via GetTask (mirrors samvel-32 fix: avoid in-memory drift of
	// updated_at/status/note in the returned struct — DB is truth after commit).
	refreshed, _, err := s.GetTask(ctx, absorberID)
	if err != nil {
		refreshed = taskA
		refreshed.BlockedBy = newBlockedBy
		refreshed.DoneWhen = newDoneWhen
		refreshed.References = newReferences
		refreshed.Note = newNote
	}
	return MergeResult{
		TaskA: refreshed, AbsorbedID: absorbedID,
		FieldsUpdated:    changedFields,
		RedirectedDepIDs: redirected,
		BStatus:          "SUPERSEDED",
		DryRun:           false,
	}, nil
}

// ---------------------------------------------------------------------------
// Merge helpers (dedup/sort/JSON serialisation)
// ---------------------------------------------------------------------------

func mergeIntSetSortedExclude(a, b []int, exclude int) []int {
	set := map[int]bool{}
	for _, v := range a {
		set[v] = true
	}
	for _, v := range b {
		set[v] = true
	}
	delete(set, exclude)
	out := make([]int, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

func mergeStringSetSorted(a, b []string) []string {
	set := map[string]bool{}
	for _, v := range a {
		set[v] = true
	}
	for _, v := range b {
		set[v] = true
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func intSliceEq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func stringSliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func toJSONArrayInts(v []int) string {
	if v == nil {
		return "[]"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func toJSONArrayStrings(v []string) string {
	if v == nil {
		return "[]"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// ---------------------------------------------------------------------------
// SubtasksFromPlan: parse Phase headers from task_plan.md and INSERT subtasks
// ---------------------------------------------------------------------------

// SubtasksFromPlanPhase reports one phase parsed from task_plan.md and its
// created subtask (id=0 in dry_run).
type SubtasksFromPlanPhase struct {
	Num       string `json:"num"`
	Title     string `json:"title"`
	SubtaskID int    `json:"subtask_id"`
	Items     int    `json:"items"`
}

// SubtasksFromPlanResult is the shape returned to the client.
type SubtasksFromPlanResult struct {
	TaskID         int                    `json:"task_id"`
	SpecPath       string                 `json:"spec_path"`
	Phases         []SubtasksFromPlanPhase `json:"phases"`
	ParentReopened bool                   `json:"parent_reopened"`
	DryRun         bool                   `json:"dry_run"`
	Failures       []AdvanceGateFailure   `json:"failures,omitempty"`
}

// phaseHeaderRE mirrors Python re.compile(r"^###?\s+Phase\s+(\d+[\.\d]*):?\s*(.*)")
// with re.MULTILINE. Go regexp needs (?m) prefix and separate flags.
var phaseHeaderRE = regexp.MustCompile(`(?m)^###?\s+Phase\s+(\d+[\.\d]*):?\s*(.*)`)

// checklistRE mirrors Python re.compile(r"^\s*-\s*\[\s*\]\s+(.+)") multi-line.
var checklistRE = regexp.MustCompile(`(?m)^\s*-\s*\[\s*\]\s+(.+)`)

// SubtasksFromPlan reads the parent's task_plan.md, parses "### Phase N: Title"
// sections + their "- [ ] item" checklists, and creates one subtask per phase
// in a single transaction. Parent is REOPENED if it was DONE.
//
// task_plan resolution:
//  1. task.TaskPlan (if non-empty) — relative to axFSRoot.
//  2. Convention fallback: docs/specs/{owner_lower}-{id}/task_plan.md.
//
// If neither exists returns 422 SPEC_MISSING (client emits helpful message).
//
// Semantics ported from ax/backlogist/core/commands.py:2896 cmd_subtasks_from_plan:
//   - done_when per subtask capped to first 10 checklist items.
//   - subtask title = "#{parent_id}.{phase_num}: {phase_title}".
//   - subtask why = "Phase {phase_num} of #{parent_id} ({parent_title})".
//   - workflow/mode/session inherited from parent (workflow defaults to "code_task").
//   - tags inherited (from a raw SELECT since not on Task struct).
//   - Parent DONE → REOPENED (client sees parent_reopened=true).
//
// Not ported:
//   - export_yaml() — native callers work through HTTP.
//   - stderr print "Parent reopened..." — parent_reopened bool suffices.
//   - Direct edit of task.task_plan when using convention fallback — the
//     Python side does that as a side effect; here we only report resolved
//     spec_path in the response so the client can PATCH if desired.
func (s *Store) SubtasksFromPlan(ctx context.Context, taskID int, agent string, dryRun bool) (SubtasksFromPlanResult, error) {
	task, _, err := s.GetTask(ctx, taskID)
	if err != nil {
		return SubtasksFromPlanResult{}, err
	}

	// Resolve task_plan path.
	rel := strings.TrimSpace(task.TaskPlan)
	if rel == "" {
		// Convention: docs/specs/{owner_lower}-{id}/task_plan.md.
		owner := strings.ToLower(strings.TrimSpace(task.Owner))
		if owner == "" {
			return SubtasksFromPlanResult{
				TaskID: taskID, DryRun: dryRun,
				Failures: []AdvanceGateFailure{{
					Check:  "SPEC_MISSING",
					Detail: "task has no task_plan and no owner for convention fallback",
				}},
			}, nil
		}
		rel = fmt.Sprintf("docs/specs/%s-%d/task_plan.md", owner, taskID)
	}
	// Best-effort git pull before reading — the file may have been pushed
	// moments ago by /prepare-task. Mirrors proxy.go:121-131 for /exec, so
	// native handlers that read FS see the same freshness guarantees as the
	// Python proxy path. 5s timeout; errors logged not returned (fall through
	// to whatever is on disk, still fresher than nothing).
	pullCtx, cancelPull := context.WithTimeout(ctx, 5*time.Second)
	pull := exec.CommandContext(pullCtx, "git", "-C", axFSRoot,
		"pull", "--rebase", "--autostash", "origin", "main")
	pull.Env = os.Environ()
	if out, err := pull.CombinedOutput(); err != nil {
		log.Printf("git pull %s failed (continuing): %s: %v",
			axFSRoot, bytes.TrimSpace(out), err)
	}
	cancelPull()

	full := filepath.Join(axFSRoot, rel)
	content, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			return SubtasksFromPlanResult{
				TaskID: taskID, SpecPath: rel, DryRun: dryRun,
				Failures: []AdvanceGateFailure{{
					Check:  "SPEC_MISSING",
					Detail: fmt.Sprintf("task_plan not found at %s", rel),
				}},
			}, nil
		}
		return SubtasksFromPlanResult{}, fmt.Errorf("read task_plan %s: %w", rel, err)
	}

	// Parse Phase headers + section content per phase.
	matches := phaseHeaderRE.FindAllSubmatchIndex(content, -1)
	if len(matches) == 0 {
		return SubtasksFromPlanResult{
			TaskID: taskID, SpecPath: rel, DryRun: dryRun,
			Phases: []SubtasksFromPlanPhase{},
		}, nil
	}

	type parsedPhase struct {
		num   string
		title string
		items []string
	}
	phases := make([]parsedPhase, 0, len(matches))
	for i, m := range matches {
		numStart, numEnd := m[2], m[3]
		titleStart, titleEnd := m[4], m[5]
		num := string(content[numStart:numEnd])
		title := strings.TrimSpace(string(content[titleStart:titleEnd]))
		if title == "" {
			title = fmt.Sprintf("Phase %s", num)
		}
		// Section = between this phase's header end and next phase's header start (or EOF).
		sectionStart := m[1] // FindAllSubmatchIndex m[1] is the end of the outermost match
		var sectionEnd int
		if i+1 < len(matches) {
			sectionEnd = matches[i+1][0]
		} else {
			sectionEnd = len(content)
		}
		section := content[sectionStart:sectionEnd]
		itemMatches := checklistRE.FindAllSubmatch(section, -1)
		items := make([]string, 0, len(itemMatches))
		for _, im := range itemMatches {
			items = append(items, strings.TrimSpace(string(im[1])))
		}
		phases = append(phases, parsedPhase{num: num, title: title, items: items})
	}

	if dryRun {
		out := make([]SubtasksFromPlanPhase, 0, len(phases))
		for _, p := range phases {
			out = append(out, SubtasksFromPlanPhase{
				Num: p.num, Title: p.title, SubtaskID: 0, Items: len(p.items),
			})
		}
		return SubtasksFromPlanResult{
			TaskID: taskID, SpecPath: rel, Phases: out, DryRun: true,
		}, nil
	}

	// Fetch parent's tags + session (not on Task struct) for inheritance.
	var tagsJSON, session string
	if err := s.pool.QueryRow(ctx,
		`SELECT tags, session FROM tasks WHERE id = $1`, taskID,
	).Scan(&tagsJSON, &session); err != nil {
		return SubtasksFromPlanResult{}, fmt.Errorf("load parent tags/session: %w", err)
	}
	// tags stored as text; pass through as-is (already valid JSON).
	workflow := task.Workflow
	if workflow == "" {
		workflow = "code_task"
	}
	mode := task.Mode
	if mode == "" {
		mode = "Code"
	}
	nowDate := time.Now().UTC().Format("2006-01-02")

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubtasksFromPlanResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Catch up the SERIAL sequence in case Python next_id() (MAX+1 pattern) has
	// been advancing IDs without incrementing the sequence. Safe/idempotent.
	if _, err := tx.Exec(ctx,
		`SELECT setval(pg_get_serial_sequence('tasks','id'),
		               COALESCE((SELECT MAX(id) FROM tasks), 1))`,
	); err != nil {
		return SubtasksFromPlanResult{}, fmt.Errorf("setval id seq: %w", err)
	}

	out := make([]SubtasksFromPlanPhase, 0, len(phases))
	for _, p := range phases {
		items := p.items
		if len(items) > 10 {
			items = items[:10]
		}
		title := fmt.Sprintf("#%d.%s: %s", taskID, p.num, p.title)
		why := fmt.Sprintf("Phase %s of #%d (%s)", p.num, taskID, task.Title)
		doneJSON := toJSONArrayStrings(items)
		var newID int
		if err := tx.QueryRow(ctx,
			`INSERT INTO tasks
			     (title, why, owner, status, mode, workflow, done_when, tags,
			      parent_task_id, session, created, created_at, updated_at)
			 VALUES ($1, $2, $3, 'BACKLOG', $4, $5, $6, $7, $8, $9, $10, NOW()::text, NOW()::text)
			 RETURNING id`,
			title, why, task.Owner, mode, workflow, doneJSON, tagsJSON,
			taskID, session, nowDate,
		).Scan(&newID); err != nil {
			return SubtasksFromPlanResult{}, fmt.Errorf("insert subtask (phase %s): %w", p.num, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'status', '', 'BACKLOG', $3, NOW()::text)`,
			newID, agent, fmt.Sprintf("subtask-from-plan (phase %s)", p.num),
		); err != nil {
			return SubtasksFromPlanResult{}, fmt.Errorf("audit subtask %d: %w", newID, err)
		}
		out = append(out, SubtasksFromPlanPhase{
			Num: p.num, Title: p.title, SubtaskID: newID, Items: len(items),
		})
	}

	// Reopen parent if it was DONE. Python cmd_subtasks_from_plan does this
	// with a stderr warning; we return parent_reopened=true and let the client
	// print an appropriate note.
	parentReopened := false
	if strings.ToUpper(strings.TrimSpace(task.Status)) == "DONE" {
		if _, err := tx.Exec(ctx,
			`UPDATE tasks SET status = 'REOPENED', updated_at = NOW()::text, closed_at = NULL WHERE id = $1`,
			taskID,
		); err != nil {
			return SubtasksFromPlanResult{}, fmt.Errorf("reopen parent: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO audit_trail (task_id, agent, field_changed, old_value, new_value, command, timestamp)
			 VALUES ($1, $2, 'status', 'DONE', 'REOPENED', 'subtasks-from-plan: parent reopened (children created)', NOW()::text)`,
			taskID, agent,
		); err != nil {
			return SubtasksFromPlanResult{}, fmt.Errorf("audit reopen: %w", err)
		}
		parentReopened = true
	}

	if err := tx.Commit(ctx); err != nil {
		return SubtasksFromPlanResult{}, fmt.Errorf("commit: %w", err)
	}
	if s.cache != nil {
		s.cache.Bump()
	}
	return SubtasksFromPlanResult{
		TaskID: taskID, SpecPath: rel, Phases: out, ParentReopened: parentReopened,
		DryRun: false,
	}, nil
}

// ---------------------------------------------------------------------------
// SMM async trigger API (#1391) — on-demand SMM without SSM
// ---------------------------------------------------------------------------

// smmRunsDir is where per-run state files live. Wrapper script writes
// state transitions here; GET /smm/runs/{id} reads them. deploy:deploy
// owned (operator ran `mkdir + chown` once at deploy time).
const smmRunsDir = "/opt/apps/ax/runs"

// smmWrapperScript is the wrapper deployed with the ax/ tree via git.
// backlog-server forks it via exec.Command; the wrapper flips state file
// queued→running before smm-daily-monitor.sh and running→done|failed after.
const smmWrapperScript = "/opt/apps/ax/scripts/smm-run.sh"

// smmReportsRoot is where smm-daily-monitor.sh drops per-client reports.
// Mirrors backend/smm/insights.py --output convention.
const smmReportsRoot = "/opt/apps/ax/output/smm/clients"

// SMMRunState is the JSON shape the wrapper script writes and GET /smm/runs
// reads. Only the wrapper writes; the server only reads.
type SMMRunState struct {
	RunID      string `json:"run_id"`
	Job        string `json:"job"`
	Status     string `json:"status"` // queued|running|done|failed
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	PID        int    `json:"pid,omitempty"`
	Agent      string `json:"agent,omitempty"`
	LogTail    string `json:"log_tail,omitempty"` // populated by GET handler, not stored
	ReportURL  string `json:"report_url,omitempty"`
}

// TriggerSMMResult is what /smm/trigger returns to clients.
type TriggerSMMResult struct {
	RunID  string `json:"run_id"`
	Job    string `json:"job"`
	Status string `json:"status"`
}

// TriggerSMM starts a background SMM pipeline run:
//  1. Assigns a run_id (timestamp + random suffix).
//  2. Writes state file /opt/apps/ax/runs/<id>.json with status=queued.
//  3. Forks smm-run.sh <run_id> <job> <agent> via setsid so it survives the
//     server restarting. Wrapper mutates the state file through the run.
//
// job = "pipeline" (full 5-step smm-daily-monitor.sh — MVP scope). "content_agent"
// (step-5-only) can be added later without protocol change.
//
// agent is recorded in the state file for audit; matches AX_AGENT of the caller.
func (s *Store) TriggerSMM(ctx context.Context, job, agent string) (TriggerSMMResult, error) {
	if job == "" {
		job = "pipeline"
	}
	if job != "pipeline" {
		return TriggerSMMResult{}, fmt.Errorf("job must be 'pipeline' (MVP scope; other jobs deferred)")
	}
	if agent == "" {
		agent = "unknown"
	}
	// Ensure runs dir exists (idempotent — operator should have chown'd it
	// once at deploy).
	if err := os.MkdirAll(smmRunsDir, 0o755); err != nil {
		return TriggerSMMResult{}, fmt.Errorf("mkdir runs: %w", err)
	}
	// run_id = YYYYMMDDTHHMMSSZ-<8hex>. Sortable, human-readable, unique.
	runID := time.Now().UTC().Format("20060102T150405Z") + "-" + randomHex8()

	// Seed queued state so GET /smm/runs/<id> works immediately even if the
	// wrapper hasn't started writing yet.
	initial := SMMRunState{
		RunID: runID, Job: job, Status: "queued", Agent: agent,
	}
	seedBytes, _ := json.MarshalIndent(initial, "", "  ")
	statePath := filepath.Join(smmRunsDir, runID+".json")
	if err := os.WriteFile(statePath, seedBytes, 0o644); err != nil {
		return TriggerSMMResult{}, fmt.Errorf("write state file: %w", err)
	}

	// Spawn wrapper. setsid + Detached so it survives backlog-server restart.
	// Nohup-style: stdout/stderr redirected to per-run log inside runs dir.
	logPath := filepath.Join(smmRunsDir, runID+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return TriggerSMMResult{}, fmt.Errorf("create log file: %w", err)
	}
	cmd := exec.Command(smmWrapperScript, runID, job, agent)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// We don't Wait — fire and forget. wrapper writes state file transitions.
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return TriggerSMMResult{}, fmt.Errorf("start wrapper: %w", err)
	}
	// After Start, we can safely close our copy of the log file — the child
	// inherited its own fd via cmd.Stdout/Stderr.
	logFile.Close()

	// Update seed with PID so GET can watchdog dead processes.
	initial.PID = cmd.Process.Pid
	seedBytes, _ = json.MarshalIndent(initial, "", "  ")
	_ = os.WriteFile(statePath, seedBytes, 0o644)

	// Detach the process from Go's tracked children — otherwise it becomes a
	// zombie when done and the server keeps a file descriptor for the pipe.
	// The wrapper writes exit_code to the state file; we don't need reap.
	go func() { _ = cmd.Wait() }()

	return TriggerSMMResult{RunID: runID, Job: job, Status: "queued"}, nil
}

// GetSMMRun reads /opt/apps/ax/runs/<id>.json + tails the log.
//
// Watchdog: if state is "running" but the PID is dead, mark as failed with
// exit_code=-1 (fixes zombie state after a wrapper crash the state file didn't
// catch). We rewrite the file so subsequent polls see stable state.
func (s *Store) GetSMMRun(ctx context.Context, runID string) (SMMRunState, error) {
	statePath := filepath.Join(smmRunsDir, runID+".json")
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return SMMRunState{}, ErrRunNotFound
		}
		return SMMRunState{}, fmt.Errorf("read state: %w", err)
	}
	var st SMMRunState
	if err := json.Unmarshal(data, &st); err != nil {
		return SMMRunState{}, fmt.Errorf("parse state: %w", err)
	}
	// Watchdog running→failed if PID is gone. kill -0 returns nil if alive.
	if st.Status == "running" && st.PID > 0 {
		if err := syscall.Kill(st.PID, 0); err != nil {
			// Process gone. Mark failed. Callers see stable status next poll.
			exitCode := -1
			st.Status = "failed"
			st.ExitCode = &exitCode
			if st.FinishedAt == "" {
				st.FinishedAt = time.Now().UTC().Format(time.RFC3339)
			}
			out, _ := json.MarshalIndent(st, "", "  ")
			_ = os.WriteFile(statePath, out, 0o644)
		}
	}
	// Populate log tail — last ~50 lines of the run log.
	logPath := filepath.Join(smmRunsDir, runID+".log")
	if logData, err := os.ReadFile(logPath); err == nil {
		st.LogTail = tailLines(string(logData), 50)
	}
	return st, nil
}

// ReadSMMReport serves output/smm/clients/<slug>/reports/<date>.json.
// Returns ErrRunNotFound if the file doesn't exist (client uses 404).
func (s *Store) ReadSMMReport(ctx context.Context, slug, date string) ([]byte, error) {
	// Basic sanitisation — path components must not contain ../
	if strings.Contains(slug, "..") || strings.Contains(slug, "/") ||
		strings.Contains(date, "..") || strings.Contains(date, "/") {
		return nil, fmt.Errorf("invalid slug or date")
	}
	full := filepath.Join(smmReportsRoot, slug, "reports", date+".json")
	data, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrRunNotFound
		}
		return nil, fmt.Errorf("read report: %w", err)
	}
	return data, nil
}

// ErrRunNotFound signals a missing run state file or missing report — client
// translates to 404.
var ErrRunNotFound = errors.New("smm run or report not found")

// randomHex8 returns 8 lowercase hex characters, used as run_id suffix.
// Not cryptographic; sync.Once seeding is fine.
func randomHex8() string {
	const hex = "0123456789abcdef"
	// crypto/rand is overkill for run_id disambiguation; time.Now nanos +
	// a tiny mix is plenty of entropy for concurrent triggers.
	n := time.Now().UnixNano()
	out := make([]byte, 8)
	for i := 0; i < 8; i++ {
		out[i] = hex[n&0xf]
		n >>= 4
	}
	return string(out)
}

// tailLines returns the last n lines of s. Cheap enough for the ~50-line tail
// the /smm/runs handler wants.
func tailLines(s string, n int) string {
	if n <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
