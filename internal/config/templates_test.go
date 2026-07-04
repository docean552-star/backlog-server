package config

import "testing"

func TestValidateTemplate_Empty_NoOp(t *testing.T) {
	if got := ValidateTemplate("", TemplateFields{}); got != nil {
		t.Errorf("empty template should be no-op, got %+v", got)
	}
}

func TestValidateTemplate_Unknown_NoOp(t *testing.T) {
	if got := ValidateTemplate("nonexistent", TemplateFields{}); got != nil {
		t.Errorf("unknown template should be no-op (Python parity), got %+v", got)
	}
}

func TestValidateTemplate_Bugfix_MissingReferences(t *testing.T) {
	got := ValidateTemplate("bugfix", TemplateFields{})
	if got == nil {
		t.Fatal("expected TEMPLATE_MISSING_FIELDS, got nil")
	}
	if got.Check != "TEMPLATE_MISSING_FIELDS" {
		t.Errorf("wrong check: %s", got.Check)
	}
}

func TestValidateTemplate_Bugfix_WithReferences_Passes(t *testing.T) {
	f := TemplateFields{References: []string{"HANDOFF_S.md"}}
	if got := ValidateTemplate("bugfix", f); got != nil {
		t.Errorf("bugfix with references should pass, got %+v", got)
	}
}

func TestValidateTemplate_Feature_MissingConsumers(t *testing.T) {
	got := ValidateTemplate("feature", TemplateFields{})
	if got == nil || got.Check != "TEMPLATE_MISSING_FIELDS" {
		t.Errorf("feature without consumers should fail, got %+v", got)
	}
}

func TestValidateTemplate_Feature_WithConsumers_Passes(t *testing.T) {
	f := TemplateFields{Consumers: []string{"marketing"}}
	if got := ValidateTemplate("feature", f); got != nil {
		t.Errorf("feature with consumers should pass, got %+v", got)
	}
}

func TestValidateTemplate_Migration_MissingNote(t *testing.T) {
	got := ValidateTemplate("migration", TemplateFields{})
	if got == nil {
		t.Fatal("expected TEMPLATE_MISSING_FIELDS")
	}
	if got.Check != "TEMPLATE_MISSING_FIELDS" {
		t.Errorf("wrong check: %s", got.Check)
	}
}

func TestValidateTemplate_Migration_WithNote_Passes(t *testing.T) {
	f := TemplateFields{MigrationNote: "adds prepare_states table"}
	if got := ValidateTemplate("migration", f); got != nil {
		t.Errorf("migration with note should pass, got %+v", got)
	}
}

func TestValidateTemplate_Migration_WhitespaceOnlyNote_Fails(t *testing.T) {
	f := TemplateFields{MigrationNote: "   "}
	if got := ValidateTemplate("migration", f); got == nil {
		t.Errorf("whitespace-only migration_note should fail")
	}
}
