package scoring

import "strings"

// DetectType mirrors detect_type from backlogist/core/scoring.py.
// Guesses task type from title and mode keywords.
// Byte-identical order of checks — first match wins.
func DetectType(t Task) string {
	mode := strings.ToLower(t.Mode)
	title := strings.ToLower(t.Title)

	// "fix" / "bug" / "broken" / "тест" / "test" → fix
	for _, kw := range []string{"fix", "bug", "broken", "тест", "test"} {
		if strings.Contains(title, kw) {
			return "fix"
		}
	}

	if mode == "think" {
		return "spec"
	}

	for _, kw := range []string{
		"planning:", "production:", "audit:", "ui", "ux", "card", "layout",
		"workspace", "panel", "modal", "tab", "compact", "redesign", "visual",
		"канбан", "календар",
	} {
		if strings.Contains(title, kw) {
			return "ui"
		}
	}

	for _, kw := range []string{
		"go api", "api:", "endpoint", "rest", "orchestrator:",
		"реальные данные", "real data", "apify", "buffer", "telegram bot",
		"dispatcher", "wordpress", "cms",
	} {
		if strings.Contains(title, kw) {
			return "integration"
		}
	}

	for _, kw := range []string{
		"docker", "ci/cd", "deploy", "backlog-gen", "doc-health",
		"doc-consolidation", "pager", "server", "auth", "scheduler", "стенд",
		"skill", "hook", "automation", "autodetect", "auto-archive", "автоматизация",
		"series detection", "id collision", "token budget", "onboarding",
		"blocked_by status", "tracking",
	} {
		if strings.Contains(title, kw) {
			return "infrastructure"
		}
	}

	for _, kw := range []string{
		"refactor", "migrate", "deprecate", "tech debt", "рефакторинг",
	} {
		if strings.Contains(title, kw) {
			return "refactor"
		}
	}

	for _, kw := range []string{
		"process", "enforcement", "structural", "deep flow",
		"human gate сценарии", "тикер:", "signal",
	} {
		if strings.Contains(title, kw) {
			return "process"
		}
	}

	return "feature"
}
