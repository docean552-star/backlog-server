package scoring

import "strings"

// AgentTypePenalties mirrors AGENT_TYPE_PENALTIES from backlogist/core/scoring.py.
// Keys are owner slugs (lowercase); inner map keys are task types produced by
// DetectType. Byte-identical values — any drift breaks parity fixtures.
var AgentTypePenalties = map[string]map[string]float64{
	"a":      {"infrastructure": 0.5, "fix": 0.5, "feature": 1.0, "spec": 3.0, "integration": 0.5, "ui": 3.0, "refactor": 1.0, "process": 2.0},
	"b":      {"infrastructure": 3.0, "fix": 1.0, "feature": 1.0, "spec": 3.0, "integration": 3.0, "ui": 0.5, "refactor": 2.0, "process": 3.0},
	"meta":   {"infrastructure": 1.0, "fix": 2.0, "feature": 2.0, "spec": 0.5, "integration": 2.0, "ui": 3.0, "refactor": 2.0, "process": 0.5},
	"t1":     {"infrastructure": 0.5, "fix": 0.5, "feature": 1.5, "spec": 2.0, "integration": 0.5, "ui": 3.0, "refactor": 1.5, "process": 2.0},
	"t2":     {"infrastructure": 3.0, "fix": 3.0, "feature": 3.0, "spec": 0.5, "integration": 1.0, "ui": 3.0, "refactor": 2.0, "process": 1.0},
	"samvel": {"infrastructure": 0.5, "fix": 0.5, "feature": 1.0, "spec": 3.0, "integration": 0.5, "ui": 3.0, "refactor": 1.5, "process": 3.0},
}

// DefaultPenalties mirrors DEFAULT_PENALTIES from backlogist/core/scoring.py.
var DefaultPenalties = map[string]float64{
	"infrastructure": 1.0, "fix": 1.0, "feature": 1.5, "spec": 2.0,
	"integration": 1.0, "ui": 2.0, "refactor": 2.0, "process": 2.0,
}

// AgentTypePenalty returns the penalty for an owner/task_type combination.
// Missing owner falls back to DefaultPenalties. Missing task_type falls back
// to 2.0 (matching Python `.get(task_type, 2.0)`).
func AgentTypePenalty(owner, taskType string) float64 {
	tbl, ok := AgentTypePenalties[strings.ToLower(owner)]
	if !ok {
		tbl = DefaultPenalties
	}
	if v, ok := tbl[taskType]; ok {
		return v
	}
	return 2.0
}
