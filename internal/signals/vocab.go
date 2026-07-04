// Package signals — vocabulary + parsing for the ticker→backlog
// `signal_context` payload. Port of the receiver side at
// ax/backlogist/core/commands.py::_parse_signal_context (:343-383) +
// _KNOWN_SIGNAL_ISSUE_TYPES set (:338-340).
//
// #1414 brief.md Q4 answer: native. This receiver does NOT reject unknown
// types — the authoritative vocabulary lives at cron.go:1180
// (ruleToIssueType) and emits a SUPERSET of the set below. Unknown type →
// SIGNAL_CONTEXT_TYPE_UNKNOWN advisory (soft note, not a 422 reject).
package signals

// KnownIssueTypes mirrors Python _KNOWN_SIGNAL_ISSUE_TYPES verbatim. New
// types added by cron.go (Go side) do not need to be added here — the
// advisory is soft. Callers use IsKnown to decide whether to emit the
// advisory.
var KnownIssueTypes = map[string]bool{
	"gap":                true,
	"content_decay":      true,
	"thin_page":          true,
	"competitive":        true,
	"keywords_declining": true,
}

// SignalContext is the JSON body of the signal_context field on POST /tasks.
// All fields are optional; unknown keys are preserved via Extra and merged
// into custom_fields.signal_context verbatim.
type SignalContext struct {
	Type         string  `json:"type,omitempty"`
	IssueType    string  `json:"issue_type,omitempty"` // alias for Type per commands.py:375
	Severity     string  `json:"severity,omitempty"`
	MoneyImpact  any     `json:"money_impact,omitempty"` // number OR object per commands.py:352
	RuleID       string  `json:"rule_id,omitempty"`
	EntityID     string  `json:"entity_id,omitempty"`
	ScanID       string  `json:"scan_id,omitempty"`
	Project      string  `json:"project,omitempty"`
	SourceData   any     `json:"source_data,omitempty"`
	URLs         []string `json:"urls,omitempty"`
	PriorityScore any    `json:"priority_score,omitempty"`
	Title        string  `json:"title,omitempty"`
	// Extra preserves any additional keys — callers merge into custom_fields
	// so the wire format stays open for cron.go to add fields without a
	// server-side deploy.
	Extra map[string]any `json:"-"`
}

// IssueType returns Type when set, else IssueType (alias). Mirrors Python
// `ctx.get("type", ctx.get("issue_type"))` at commands.py:375.
func (s *SignalContext) IssueType_() string {
	if s == nil {
		return ""
	}
	if s.Type != "" {
		return s.Type
	}
	return s.IssueType
}

// IsKnown reports whether typ is in the receiver-side known set (i.e. we
// don't need an advisory). Empty string → true (no type = no advisory).
func IsKnown(typ string) bool {
	if typ == "" {
		return true
	}
	return KnownIssueTypes[typ]
}
