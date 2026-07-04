package config

// TASKOWNERS registry + rule engine — Go port of
// ax/backlogist/core/taskowners.py. Loads declarative routing rules from
// taskowners.yaml, matches tasks to owners by field patterns, and returns
// the best-fit rule per specificity + priority + capacity.
//
// Cache pattern (mirrors SubtasksFromPlan git-pull-before-handler):
//   - LoadTaskowners: parse once at startup.
//   - Reload: best-effort `git pull --rebase --autostash origin main` +
//     os.Stat mtime check. Skip reparse when file unchanged. sync.RWMutex
//     guards the rules slice so concurrent MatchRule reads share one snapshot.
//   - Single-flight: sync.Mutex on the reload path ensures N concurrent
//     Reload() calls share one git-pull + parse (mirror Python single-flight
//     via file lock elsewhere).

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// OwnerRule is one declarative routing rule from taskowners.yaml. Field names
// mirror Python OwnerRule dataclass verbatim so hand-comparison stays cheap.
type OwnerRule struct {
	Patterns        map[string]any `yaml:"patterns"`
	Owner           string         `yaml:"owner"`
	Priority        int            `yaml:"priority"`
	RequiredAgents  []string       `yaml:"required_agents"`
	RequiredReviews []string       `yaml:"required_reviews"`
	Workflow        string         `yaml:"workflow"`
}

// yamlTaskowners is the on-disk shape: a top-level `rules:` list.
type yamlTaskowners struct {
	Rules []yamlRule `yaml:"rules"`
}

// yamlRule uses a permissive shape (RequiredAgents/RequiredReviews can be
// string OR []string) matching Python's isinstance coercion at
// taskowners.py:71-74. Post-parse we normalise into OwnerRule.
type yamlRule struct {
	Patterns        map[string]any `yaml:"patterns"`
	Owner           string         `yaml:"owner"`
	Priority        int            `yaml:"priority"`
	RequiredAgents  any            `yaml:"required_agents"`
	RequiredReviews any            `yaml:"required_reviews"`
	Workflow        string         `yaml:"workflow"`
}

// TaskownersRegistry holds the parsed rule set + mtime + git-pull machinery.
// Reads take RLock; Reload takes the reloadMu single-flight lock + write lock
// on rules when swapping in the new slice. Zero value is unusable — call
// LoadTaskowners.
type TaskownersRegistry struct {
	dir       string       // absolute path to the ax/ checkout containing taskowners.yaml
	rules     []OwnerRule  // sorted specificity desc, stable
	mtime     time.Time    // last stat mtime of taskowners.yaml
	lastPull  time.Time    // last successful git pull (debounce, skip if <60s)
	mu        sync.RWMutex // guards rules + mtime + lastPull
	reloadMu  sync.Mutex   // single-flight around git-pull + parse
}

// pullDebounce caps the git-pull frequency. Each POST /tasks calls Reload,
// but pulling on every request adds ~1s of network round-trip that dwarfs
// the 100-200ms native latency target. 60s debounce is generous — freshly
// pushed rules take at most a minute to appear, and a manual `SIGHUP`
// path is a follow-up ticket if the operator ever needs zero-lag reload.
const pullDebounce = 60 * time.Second

// LoadTaskowners parses taskowners.yaml from dir once and returns the
// registry ready for MatchRule calls. Missing file → empty rule set (no
// error; mirrors Python load_rules:41). Malformed YAML → returns error.
func LoadTaskowners(dir string) (*TaskownersRegistry, error) {
	r := &TaskownersRegistry{dir: dir}
	if err := r.reparseLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

// path returns the on-disk taskowners.yaml location.
func (r *TaskownersRegistry) path() string {
	return filepath.Join(r.dir, "taskowners.yaml")
}

// reparseLocked reads, unmarshals, normalises and sort-installs rules.
// Caller MUST hold reloadMu. Skips gracefully when the file is absent.
func (r *TaskownersRegistry) reparseLocked() error {
	p := r.path()
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			// Missing file → empty rules; keep zero mtime so any create
			// eventually reparses if the file appears.
			r.mu.Lock()
			r.rules = nil
			r.mtime = time.Time{}
			r.mu.Unlock()
			return nil
		}
		return fmt.Errorf("stat %s: %w", p, err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("read %s: %w", p, err)
	}
	var doc yamlTaskowners
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", p, err)
	}
	rules := make([]OwnerRule, 0, len(doc.Rules))
	for i, raw := range doc.Rules {
		if strings.TrimSpace(raw.Owner) == "" {
			return fmt.Errorf("TASKOWNERS syntax error: rule #%d missing 'owner'", i)
		}
		wf := raw.Workflow
		if wf == "" {
			wf = "code_task"
		}
		rules = append(rules, OwnerRule{
			Patterns:        raw.Patterns,
			Owner:           raw.Owner,
			Priority:        raw.Priority,
			RequiredAgents:  coerceStringList(raw.RequiredAgents),
			RequiredReviews: coerceStringList(raw.RequiredReviews),
			Workflow:        wf,
		})
	}
	// Specificity sort desc, stable (mirror taskowners.py:83).
	sort.SliceStable(rules, func(i, j int) bool {
		return len(rules[i].Patterns) > len(rules[j].Patterns)
	})
	r.mu.Lock()
	r.rules = rules
	r.mtime = fi.ModTime()
	r.mu.Unlock()
	return nil
}

// coerceStringList mirrors Python's `if isinstance(x, str): x = [x]` at
// taskowners.py:71-74. Accepts string, []any of strings, or nil.
func coerceStringList(v any) []string {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		if s == "" {
			return nil
		}
		return []string{s}
	}
	if lst, ok := v.([]any); ok {
		out := make([]string, 0, len(lst))
		for _, e := range lst {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Reload does a best-effort git-pull + mtime check + reparse. Mirrors the
// SubtasksFromPlan (store.go:2717-2730) pattern: 5s timeout on pull; log
// and continue on failure so a stale origin never wedges the request.
// Under single-flight (reloadMu) N concurrent creates share one pull+parse.
func (r *TaskownersRegistry) Reload(ctx context.Context) error {
	// Fast-path: if a git pull ran within the debounce window, skip both
	// the git call AND the mtime stat. This is what keeps POST /tasks
	// on the 100-200ms native budget instead of paying 1-2s of network
	// per request. Any mtime bump that arrived out of band still gets
	// picked up at the next post-debounce request.
	r.mu.RLock()
	recentPull := time.Since(r.lastPull) < pullDebounce
	r.mu.RUnlock()
	if recentPull {
		return nil
	}

	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	// Re-check debounce under the reload lock — another goroutine may
	// have just refreshed while we were queued.
	r.mu.RLock()
	if time.Since(r.lastPull) < pullDebounce {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	// Best-effort git pull. Same three log-format strings as SubtasksFromPlan
	// so log-scrapers (DOC_SYSTEM/scripts/build-lint.py) stay unaffected.
	pullCtx, cancelPull := context.WithTimeout(ctx, 5*time.Second)
	pull := exec.CommandContext(pullCtx, "git", "-C", r.dir,
		"pull", "--rebase", "--autostash", "origin", "main")
	pull.Env = os.Environ()
	if out, err := pull.CombinedOutput(); err != nil {
		log.Printf("git pull %s failed (continuing): %s: %v",
			r.dir, bytes.TrimSpace(out), err)
	}
	cancelPull()
	// Mark lastPull even on failure — we still don't want to hammer the
	// remote on a broken origin. Next debounce window will retry.
	r.mu.Lock()
	r.lastPull = time.Now()
	r.mu.Unlock()

	// mtime guard — skip full reparse when nothing changed.
	fi, err := os.Stat(r.path())
	if err != nil {
		if os.IsNotExist(err) {
			return nil // file simply not present; keep whatever we have
		}
		return fmt.Errorf("stat: %w", err)
	}
	r.mu.RLock()
	same := fi.ModTime().Equal(r.mtime)
	r.mu.RUnlock()
	if same {
		return nil
	}
	return r.reparseLocked()
}

// Rules returns a snapshot of the current rule slice (safe for read-only use;
// do not mutate the returned slice or any OwnerRule.Patterns map).
func (r *TaskownersRegistry) Rules() []OwnerRule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]OwnerRule, len(r.rules))
	copy(out, r.rules)
	return out
}

// TaskFacts is the subset of a task needed for pattern matching. Field names
// mirror Python Task attributes referenced by _field_matches at
// taskowners.py:91-131. Populate via CreateTaskRequest or read from an
// existing tasks row.
type TaskFacts struct {
	Title     string
	Owner     string
	Mode      string
	Status    string
	Tags      []string
	Consumers []string
}

// fieldMatches ports Python _field_matches (taskowners.py:91-131) verbatim.
// Returns true iff task.field satisfies the expected pattern value.
// Supported fields: tags, consumers, mode, title_contains, owner, status.
// Unknown fields → no match (Python:130-131).
func fieldMatches(t TaskFacts, field string, expected any) bool {
	switch field {
	case "tags":
		lc := lowerList(t.Tags)
		return anyOverlap(lc, expected)
	case "consumers":
		lc := lowerList(t.Consumers)
		return anyOverlap(lc, expected)
	case "mode":
		return equalCI(t.Mode, expected)
	case "title_contains":
		if s, ok := expected.(string); ok {
			return strings.Contains(strings.ToLower(t.Title), strings.ToLower(s))
		}
		return false
	case "owner":
		return equalCI(t.Owner, expected)
	case "status":
		return equalCI(t.Status, expected)
	}
	return false
}

// anyOverlap: expected string in lc, or any element of expected []any in lc.
func anyOverlap(lc []string, expected any) bool {
	if s, ok := expected.(string); ok {
		return containsCI(lc, s)
	}
	if lst, ok := expected.([]any); ok {
		for _, e := range lst {
			if s, ok := e.(string); ok && containsCI(lc, s) {
				return true
			}
		}
	}
	return false
}

func containsCI(lc []string, s string) bool {
	needle := strings.ToLower(s)
	for _, v := range lc {
		if v == needle {
			return true
		}
	}
	return false
}

func lowerList(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(s)
	}
	return out
}

func equalCI(a string, expected any) bool {
	s, ok := expected.(string)
	if !ok {
		return false
	}
	return strings.EqualFold(a, s)
}

// ruleMatches returns true iff every pattern in rule.Patterns holds.
// Empty patterns = catch-all (Python:136-137).
func ruleMatches(t TaskFacts, rule OwnerRule) bool {
	if len(rule.Patterns) == 0 {
		return true
	}
	for k, v := range rule.Patterns {
		if !fieldMatches(t, k, v) {
			return false
		}
	}
	return true
}

// CapacityFn returns the number of IN_PROGRESS tasks for owner. Used as the
// third sort key when specificity + priority tie. Pass nil to skip capacity
// weighting (mirrors Python `db=None` branch at _get_capacity:203-204).
type CapacityFn func(owner string) int

// MatchRule returns the best-fit rule for task, or nil if no rule matches.
// Resolution order (Python match_rule:297-323):
//  1. Filter to rules whose patterns all match.
//  2. Highest specificity (most patterns).
//  3. Highest explicit priority.
//  4. Lowest owner capacity (fewer IN_PROGRESS = more available).
//  5. First rule in list (stable).
func (r *TaskownersRegistry) MatchRule(t TaskFacts, cap CapacityFn) *OwnerRule {
	r.mu.RLock()
	rules := r.rules
	r.mu.RUnlock()
	if len(rules) == 0 {
		return nil
	}
	candidates := make([]OwnerRule, 0, len(rules))
	for _, rule := range rules {
		if ruleMatches(t, rule) {
			candidates = append(candidates, rule)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	capCache := map[string]int{}
	getCap := func(owner string) int {
		if cap == nil {
			return 0
		}
		if v, ok := capCache[owner]; ok {
			return v
		}
		v := cap(owner)
		capCache[owner] = v
		return v
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		si, sj := len(candidates[i].Patterns), len(candidates[j].Patterns)
		if si != sj {
			return si > sj
		}
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		return getCap(candidates[i].Owner) < getCap(candidates[j].Owner)
	})
	winner := candidates[0]
	return &winner
}

// SuggestMatches returns up to limit owner suggestions (highest specificity
// first) for cases where the request omitted --auto-assign but partial
// matches exist. Used by handleCreate to emit the OWNER_SUGGESTION advisory
// list (design.md § Failure aggregation).
func (r *TaskownersRegistry) SuggestMatches(t TaskFacts, limit int) []string {
	if limit <= 0 {
		limit = 5
	}
	r.mu.RLock()
	rules := r.rules
	r.mu.RUnlock()
	seen := map[string]bool{}
	out := make([]string, 0, limit)
	for _, rule := range rules {
		if !ruleMatches(t, rule) {
			continue
		}
		if seen[rule.Owner] {
			continue
		}
		seen[rule.Owner] = true
		out = append(out, rule.Owner)
		if len(out) >= limit {
			break
		}
	}
	return out
}
