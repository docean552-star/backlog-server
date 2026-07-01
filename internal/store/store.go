package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/docean552-star/backlog-server/internal/cache"
)

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
// The scope of gate checks in MVP is intentionally narrow (code_task only,
// PLANNING → READY only): done_when non-empty + spec-reviewer PASS. File-based
// checks (research.md content quality, task_plan KQ/TS count, agent markers)
// remain a client-side concern — client should refuse to POST /advance if its
// own local check finds those missing.
func (s *Store) AdvanceTask(ctx context.Context, taskID int, agent string) (AdvanceResult, error) {
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

	// Server-side DB gates. MVP covers PLANNING → READY. Other transitions
	// pass through with no server-side check (client-side gates still enforce).
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
	if len(failures) > 0 {
		return AdvanceResult{Task: task, FromStatus: fromStatus, ToStatus: to, Failures: failures}, nil
	}

	// Passed. UPDATE tasks + INSERT audit_trail atomically.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after commit

	if _, err := tx.Exec(ctx,
		`UPDATE tasks SET status = $1, updated_at = NOW()::text WHERE id = $2`,
		to, taskID,
	); err != nil {
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

	updated := task
	updated.Status = to
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
	updated := task
	updated.Status = "IN_PROGRESS"
	updated.Owner = agent
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
	updated := task
	updated.Status = "CANCELLED"
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
	updated := task
	updated.Status = "SUPERSEDED"
	updated.Note = newNote
	return SupersedeResult{Task: updated, FromStatus: from, ToStatus: "SUPERSEDED", ByID: byID}, nil
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
	updated := task
	updated.Status = "READY"
	return AdvanceResult{Task: updated, FromStatus: from, ToStatus: "READY"}, nil
}
