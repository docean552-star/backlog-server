// Package ears — EARS (Easy Approach to Requirements Syntax, Mavin 2009)
// classifier for done_when / business_value criteria. Port of
// ax/backlogist/core/transition_gates.py::_classify_ears + _check_ears_criterion.
//
// Priority order (first match wins) — matches Python verbatim so parity tests
// on the same corpus produce identical verdicts:
//
//  1. unwanted   — IF ... THEN
//  2. state      — WHILE ...
//  3. event      — WHEN ...
//  4. optional   — WHERE ...
//  5. ubiquitous — SHALL anywhere (no leading EARS keyword)
//  6. non-ears   — none of the above
//
// #1414 uses this on business_value at create time (brief.md Q3 override:
// advisory only, never blocks). See AC 5 in requirements.md.
package ears

import (
	"regexp"
	"strings"
)

// Kind is the classification outcome.
type Kind string

const (
	Unwanted   Kind = "unwanted"
	State      Kind = "state"
	Event      Kind = "event"
	Optional   Kind = "optional"
	Ubiquitous Kind = "ubiquitous"
	NonEARS    Kind = "non-ears"
)

// The five recognisers — compiled once at package init. Python patterns
// are copied verbatim (lowercase input, case-sensitive regex to match the
// existing IEEE convention documented at transition_gates.py:2214-2216).
var (
	unwantedRE = regexp.MustCompile(`^if\s+.+\bthen\b`)
	stateRE    = regexp.MustCompile(`^while\s+`)
	eventRE    = regexp.MustCompile(`^when\s+`)
	optionalRE = regexp.MustCompile(`^where\s+`)
	shallRE    = regexp.MustCompile(`\bshall\b`)
)

// Classify returns the EARS pattern for text, or NonEARS.
// Mirrors _classify_ears at transition_gates.py:2198-2229.
func Classify(text string) Kind {
	c := strings.ToLower(strings.TrimSpace(text))
	switch {
	case unwantedRE.MatchString(c):
		return Unwanted
	case stateRE.MatchString(c):
		return State
	case eventRE.MatchString(c):
		return Event
	case optionalRE.MatchString(c):
		return Optional
	case shallRE.MatchString(c):
		return Ubiquitous
	}
	return NonEARS
}

// MatchesEARS reports whether text is a well-formed EARS statement
// (a recognised kind + a SHALL). Used by handleCreate to decide whether to
// emit the EARS_ADVISORY_BV advisory (brief.md Q3).
//
// Returns false in two cases:
//  1. NonEARS (no recognised keyword and no SHALL) → advisory "not EARS-formatted"
//  2. Recognised as EARS pattern by leading keyword but SHALL missing
//     ("should"/"must"/"will" are not SHALL-equivalent per Mavin) →
//     advisory "classified as X but missing SHALL keyword"
//
// Empty/whitespace-only text returns false (no valid EARS).
func MatchesEARS(text string) bool {
	c := strings.ToLower(strings.TrimSpace(text))
	if c == "" {
		return false
	}
	kind := Classify(c)
	if kind == NonEARS {
		return false
	}
	// Even a recognised kind must carry SHALL.
	return shallRE.MatchString(c)
}
