// Package scoring is a byte-identical Go port of
// backlogist/core/scoring.py (task #1730).
//
// This package is authoritative for scoring computation on the native
// `AdvanceTask` path. The Python module remains the authoritative reference
// (byte-identical parity is required — see internal/scoring/parity_fixtures.json)
// and continues to serve `backlogist rescore` and the Python `cmd_close` path.
//
// ANY change to backlogist/core/scoring.py MUST be reflected here and vice
// versa. The 12 parity fixtures catch drift at test time.
package scoring

// TaskScore mirrors backlogist.core.models.TaskScore. The `Impact` pointer
// distinguishes "never rated" (nil) from an explicit rating of 0 (edge case
// per Python's `Optional[int]` sentinel semantics).
type TaskScore struct {
	Impact     *int `json:"impact,omitempty"` // 1-10 or nil
	Confidence int  `json:"confidence"`
	Ease       int  `json:"ease"`
	Dependency int  `json:"dependency"`
	Total      int  `json:"total"`
}

// Stage mirrors an entry in backlogist Task.stages ({name, owner, status}).
type Stage struct {
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Status string `json:"status"`
}

// Task is the subset of backlogist.core.models.Task needed for scoring.
// Field names use Python JSON tags for compatibility with parity_fixtures.json
// and any JSON dumps the server exchanges with Python.
type Task struct {
	ID               int        `json:"id"`
	Title            string     `json:"title"`
	Owner            string     `json:"owner"`
	Mode             string     `json:"mode"`
	Type             string     `json:"type,omitempty"`
	Status           string     `json:"status"`
	Created          string     `json:"created"`
	Why              string     `json:"why"`
	Note             string     `json:"note"`
	Session          string     `json:"session"`
	BlockedBy        []int      `json:"blocked_by"`
	Consumers        []string   `json:"consumers"`
	PriorityOverride int        `json:"priority_override"`
	TaskPlan         string     `json:"task_plan"`
	Spec             string     `json:"spec"`
	References       []string   `json:"references"`
	Score            *TaskScore `json:"score,omitempty"`
	Stages           []Stage    `json:"stages,omitempty"`
	ClientID         *int       `json:"client_id,omitempty"`
	CapabilityID     *string    `json:"capability_id,omitempty"`

	// Output fields (mutated by RecalculateAll):
	EffectiveScore float64        `json:"effective_score,omitempty"`
	Breakdown      ScoreBreakdown `json:"score_breakdown,omitempty"`
}

// ScoreBreakdown mirrors backlogist.core.models.ScoreBreakdown.
type ScoreBreakdown struct {
	Base           float64 `json:"base"`
	BlockerValue   float64 `json:"blocker_value"`
	ImpactPoints   float64 `json:"impact_points"`
	Subjective     float64 `json:"subjective"`
	ReleaseBonus   int     `json:"release_bonus"`
	BrokenBonus    int     `json:"broken_bonus"`
	ConsumersBonus int     `json:"consumers_bonus"`
	Potential      float64 `json:"potential"`
	Unblocks       int     `json:"unblocks"`
	IsBlocked      bool    `json:"is_blocked"`
	HasSpec        bool    `json:"has_spec"`
	Type           string  `json:"type"`
}

// Result mirrors one entry of the Python `_compute_all_scores` return list.
// Same fields as ScoreBreakdown plus id + effective (denormalised for the
// per-task consumer).
type Result struct {
	ID             int     `json:"id"`
	Effective      float64 `json:"effective"`
	Potential      float64 `json:"potential"`
	Base           float64 `json:"base"`
	ImpactPoints   float64 `json:"impact_points"`
	Subjective     float64 `json:"subjective"`
	ReleaseBonus   int     `json:"release_bonus"`
	BrokenBonus    int     `json:"broken_bonus"`
	ConsumersBonus int     `json:"consumers_bonus"`
	BlockerValue   float64 `json:"blocker_value"`
	Unblocks       int     `json:"unblocks"`
	IsBlocked      bool    `json:"is_blocked"`
	HasSpec        bool    `json:"has_spec"`
	Type           string  `json:"type"`
}
