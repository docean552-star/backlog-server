package config

// Template validators — port of _TEMPLATE_REQUIREMENTS + validate_template
// at ax/backlogist/core/validation.py:150-154 + :287-299.
//
// One-to-one field-presence rules; Python source of truth used (brief.md
// task_plan mislabeled the required fields).

import (
	"fmt"
	"strings"
)

// templateRequirements mirrors _TEMPLATE_REQUIREMENTS (validation.py:150).
// One entry per template name → the field it demands be non-empty. Extending
// the map here is enough — no code changes needed.
var templateRequirements = map[string]string{
	"bugfix":    "references",
	"feature":   "consumers",
	"migration": "migration_note",
}

// TemplateFields is the read-side view CreateTaskRequest exposes to the
// validator. Only fields the template check might reference are named; the
// full request carries many more.
type TemplateFields struct {
	References    []string
	Consumers     []string
	MigrationNote string
}

// TemplateFailure is one 422 entry raised when a template's required field
// is empty. Callers assemble multiple into failures[] via bulk aggregation.
type TemplateFailure struct {
	Check  string
	Detail string
}

// ValidateTemplate runs the required-field check for template. Empty template
// name is a no-op (no template selected). Unknown template is also a no-op
// with no error — mirrors Python `_TEMPLATE_REQUIREMENTS.get(...)` semantics
// where a missing key yields None and validate_template returns early
// (validation.py:293-296).
//
// Returns a non-nil failure only when the template's required field is
// empty. Bulk aggregation with other checks (custom workflow, EARS, etc.)
// happens in the caller.
func ValidateTemplate(template string, f TemplateFields) *TemplateFailure {
	t := strings.TrimSpace(template)
	if t == "" {
		return nil
	}
	required, ok := templateRequirements[t]
	if !ok {
		return nil // unknown template name → no constraint (Python parity)
	}
	if isRequiredFieldEmpty(required, f) {
		return &TemplateFailure{
			Check:  "TEMPLATE_MISSING_FIELDS",
			Detail: fmt.Sprintf("template %q requires: %s", t, required),
		}
	}
	return nil
}

func isRequiredFieldEmpty(field string, f TemplateFields) bool {
	switch field {
	case "references":
		return len(f.References) == 0
	case "consumers":
		return len(f.Consumers) == 0
	case "migration_note":
		return strings.TrimSpace(f.MigrationNote) == ""
	}
	// Unknown required field name → treat as "not required" (defensive; keeps
	// a mis-typed templateRequirements entry from silently failing every
	// create).
	return false
}
