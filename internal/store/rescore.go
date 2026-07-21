package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/docean552-star/backlog-server/internal/scoring"
	"github.com/jackc/pgx/v5"
)

// rescoreAfterDone is the Go port of Python cmd_close's recalculate_unblocked
// hook. Called from AdvanceTask when transitioning a task to DONE, inside the
// same transaction so the state change + score recompute are atomic (task
// #1730).
//
// Steps:
//  1. SELECT all active (non-terminal) tasks with FOR UPDATE — the just-DONE
//     row was already updated by the caller, but its NEW status is only
//     visible inside this tx (READ COMMITTED sees own writes). We filter it
//     out client-side to match Python semantics (recalculate_unblocked treats
//     the closed task as already-gone).
//  2. Feed the slice through scoring.RecalculateUnblocked which mutates
//     BlockedBy in place, computes new effective_score, and returns the IDs
//     of tasks that became fully unblocked.
//  3. Batch UPDATE only the tasks whose blocked_by or effective_score
//     changed. Uses a single VALUES-based UPDATE for O(1) roundtrips
//     regardless of task count (see updateScoresBatch).
//
// Failure inside this function surfaces to the AdvanceTask caller which
// returns the error and triggers `defer tx.Rollback` — no partial persist.
func (s *Store) rescoreAfterDone(ctx context.Context, tx pgx.Tx, closedID int) error {
	tasks, priorScores, priorBlocked, err := loadTasksForScoring(ctx, tx, closedID)
	if err != nil {
		return fmt.Errorf("load active tasks: %w", err)
	}
	if len(tasks) == 0 {
		// No other active tasks — nothing to rescore.
		return nil
	}

	// Compute in-memory. capability_status lookup left as no-op for MVP —
	// the field isn't populated in the current tasks schema, so the
	// dial-driven impact_points path is what fires (byte-identical to
	// Python when capability_id is absent).
	scoring.RecalculateUnblocked(closedID, tasks, "", scoring.NoCapabilityStatus)

	// Persist only tasks whose score or blocked_by actually changed. This
	// avoids UPDATE storms for the common case (single DONE cascades to
	// one or two children only).
	var changed []scoring.Task
	for _, t := range tasks {
		oldScore, hadScore := priorScores[t.ID]
		oldBlocked := priorBlocked[t.ID]
		blockedChanged := !intSliceEqual(t.BlockedBy, oldBlocked)
		scoreChanged := !hadScore || floatEq(oldScore, t.EffectiveScore, 0.001) == false
		if blockedChanged || scoreChanged {
			changed = append(changed, t)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	if err := updateScoresBatch(ctx, tx, changed); err != nil {
		return fmt.Errorf("persist scores: %w", err)
	}
	return nil
}

// loadTasksForScoring reads the minimum column set needed for the scoring
// algorithm, excluding terminal statuses AND the just-closed task itself.
// Also returns two maps for change-detection (prior effective_score + prior
// blocked_by) so the caller can persist only rows that actually moved.
func loadTasksForScoring(ctx context.Context, tx pgx.Tx, excludeID int) ([]scoring.Task, map[int]float64, map[int][]int, error) {
	// FOR UPDATE prevents concurrent DONE'ers from clobbering each other's
	// score writes. Two DONE'ers racing on overlapping descendants must
	// serialize.
	q := `SELECT id, title, owner, mode, COALESCE(why, ''), COALESCE(note, ''),
	             COALESCE(session, ''), COALESCE(created, ''), status,
	             COALESCE(blocked_by, '[]'), COALESCE(consumers, '[]'),
	             priority_override, COALESCE(task_plan, ''),
	             COALESCE("references", '[]'),
	             effective_score::text::float8
	      FROM tasks
	      WHERE status NOT IN ('DONE', 'CANCELLED', 'SUPERSEDED', 'WONT-DO', 'MERGED')
	        AND id != $1
	      ORDER BY id
	      FOR UPDATE`
	rows, err := tx.Query(ctx, q, excludeID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	var tasks []scoring.Task
	priorScore := map[int]float64{}
	priorBlocked := map[int][]int{}
	for rows.Next() {
		var t scoring.Task
		var blockedRaw, consumersRaw, refsRaw string
		var score float64
		if err := rows.Scan(
			&t.ID, &t.Title, &t.Owner, &t.Mode, &t.Why, &t.Note,
			&t.Session, &t.Created, &t.Status,
			&blockedRaw, &consumersRaw,
			&t.PriorityOverride, &t.TaskPlan, &refsRaw,
			&score,
		); err != nil {
			return nil, nil, nil, err
		}
		t.BlockedBy = parseIntArray(blockedRaw)
		t.Consumers = parseStringArray(consumersRaw)
		t.References = parseStringArray(refsRaw)
		t.EffectiveScore = score
		priorScore[t.ID] = score
		priorBlocked[t.ID] = append([]int(nil), t.BlockedBy...)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return tasks, priorScore, priorBlocked, nil
}

// updateScoresBatch persists effective_score + blocked_by for each task in a
// single roundtrip via a UPDATE ... FROM (VALUES ...) pattern.
//
// Postgres parameter limit is 65535; each task uses 3 params → cap 21k tasks
// per call (production task volumes are ~1000-2000, well within budget).
func updateScoresBatch(ctx context.Context, tx pgx.Tx, tasks []scoring.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(`UPDATE tasks
	   SET effective_score = v.score,
	       blocked_by = v.blocked::jsonb,
	       updated_at = NOW()::text
	   FROM (VALUES `)
	args := make([]any, 0, len(tasks)*3)
	for i, t := range tasks {
		if i > 0 {
			b.WriteString(", ")
		}
		blockedJSON, err := json.Marshal(t.BlockedBy)
		if err != nil {
			return fmt.Errorf("marshal blocked_by #%d: %w", t.ID, err)
		}
		fmt.Fprintf(&b, "($%d::int, $%d::float8, $%d::text)", i*3+1, i*3+2, i*3+3)
		args = append(args, t.ID, t.EffectiveScore, string(blockedJSON))
	}
	b.WriteString(`) AS v(id, score, blocked)
	   WHERE tasks.id = v.id`)
	_, err := tx.Exec(ctx, b.String(), args...)
	return err
}

func intSliceEqual(a, b []int) bool {
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

func floatEq(a, b, eps float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= eps
}
