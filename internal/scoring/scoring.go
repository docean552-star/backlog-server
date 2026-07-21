package scoring

import (
	"math"
	"strings"
	"time"
)

// CapabilityStatusFunc mirrors capability_status(capability_id) from Python.
// Returns nil if the capability_id is unknown or unset. Injected as a
// function to keep this package free of file-IO / config-catalog concerns.
// The store layer supplies the concrete implementation reading from
// config/capabilities/*.yaml (matching the Python side).
type CapabilityStatusFunc func(capabilityID string) *int

// NoCapabilityStatus is a nil-lookup default that always returns nil —
// used by tests / fixtures that exercise the dial-driven impact_points path.
func NoCapabilityStatus(capabilityID string) *int { return nil }

// nextFriday returns the ISO date of the next Friday (used as default
// release_date when caller passes "").
func nextFriday() string {
	today := time.Now().UTC()
	// Python: (4 - today.weekday()) % 7  (weekday(): Monday=0..Sunday=6)
	// Go: time.Weekday: Sunday=0..Saturday=6 → convert.
	py := (int(today.Weekday()) + 6) % 7 // Monday=0..Sunday=6
	daysAhead := (4 - py + 7) % 7
	fri := today.AddDate(0, 0, daysAhead)
	return fri.Format("2006-01-02")
}

// parseReleaseDays returns days from today until releaseDate (min 0),
// or 30 as fallback (matches Python).
func parseReleaseDays(releaseDate string) int {
	if releaseDate == "" {
		return 30
	}
	rel, err := time.Parse("2006-01-02", releaseDate)
	if err != nil {
		return 30
	}
	today := time.Now().UTC()
	// Zero-out time components for date-only comparison.
	todayDate := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
	relDate := time.Date(rel.Year(), rel.Month(), rel.Day(), 0, 0, 0, 0, time.UTC)
	days := int(relDate.Sub(todayDate).Hours() / 24)
	if days < 0 {
		return 0
	}
	return days
}

// roundHalfEven mirrors Python's built-in round(x, 1) — banker's rounding.
// Go's math.Round is half-away-from-zero; math.RoundToEven gives half-to-even
// but only at integer precision, so we shift-and-round.
func roundHalfEven(x float64, decimals int) float64 {
	mult := math.Pow(10, float64(decimals))
	return math.RoundToEven(x*mult) / mult
}

// activeBlockerIDs mirrors _active_blocker_ids — task.blocked_by is already
// scrubbed of resolved entries (see Python storage layer), so this is a
// simple copy for symmetry with the Python source.
func activeBlockerIDs(t Task) []int {
	out := make([]int, len(t.BlockedBy))
	copy(out, t.BlockedBy)
	return out
}

// RecalculateAll ports scoring._compute_all_scores. Returns per-task results
// with all breakdown fields. Byte-identical to Python within roundHalfEven
// tolerance (parity fixtures assert this).
//
// capLookup: how to resolve capability_status; pass NoCapabilityStatus when
// the caller does not want the client+capability=40 branch to fire.
func RecalculateAll(tasks []Task, releaseDate string, capLookup CapabilityStatusFunc) []Result {
	if releaseDate == "" {
		releaseDate = nextFriday()
	}
	if capLookup == nil {
		capLookup = NoCapabilityStatus
	}
	daysToRelease := parseReleaseDays(releaseDate)

	// Step 0: reverse-dep map.
	blocksMap := map[int][]int{}
	for _, t := range tasks {
		for _, b := range activeBlockerIDs(t) {
			blocksMap[b] = append(blocksMap[b], t.ID)
		}
	}

	// Step 1: base scores.
	type baseEntry struct {
		base           float64
		impactPoints   float64
		subjective     float64
		releaseBonus   int
		brokenBonus    int
		consumersBonus int
		hasSpec        bool
		isBlocked      bool
		taskType       string
	}
	baseScores := map[int]baseEntry{}

	for _, t := range tasks {
		tid := t.ID
		isBlocked := len(activeBlockerIDs(t)) > 0

		// has_spec detection.
		hasSpec := t.TaskPlan != ""
		if !hasSpec {
			for _, r := range t.References {
				if strings.Contains(strings.ToUpper(r), "SPEC") {
					hasSpec = true
					break
				}
			}
		}

		// impact_points: 40 / 28 / 20 / dial*4.
		var impactPoints float64
		hasClient := t.ClientID != nil
		var capStatus *int
		if t.CapabilityID != nil {
			capStatus = capLookup(*t.CapabilityID)
		}
		hasCapability := capStatus != nil && *capStatus >= 2
		switch {
		case hasClient && hasCapability:
			impactPoints = 40.0
		case hasClient:
			impactPoints = 28.0
		case hasCapability:
			impactPoints = 20.0
		default:
			if t.Score != nil && t.Score.Impact != nil {
				impactPoints = roundHalfEven(float64(*t.Score.Impact)*4.0, 1)
			} else {
				impactPoints = 0.0
			}
		}

		taskType := DetectType(t)

		// Subjective (per-agent, per-type). Stage owner overrides Task owner
		// if a non-terminal stage exists (mirrors Python:337-341).
		owner := strings.ToLower(t.Owner)
		if owner == "" {
			owner = "unknown"
		}
		for _, s := range t.Stages {
			if s.Status != "DONE" && s.Status != "BLOCKED" {
				if s.Owner != "" {
					owner = strings.ToLower(s.Owner)
				}
				break
			}
		}
		penalty := AgentTypePenalty(owner, taskType)
		subjective := roundHalfEven(20.0/(1.0+penalty), 1)

		// release_bonus (sprint tasks only).
		releaseBonus := 0
		session := t.Session
		created := t.Created
		status := t.Status
		isSprint := strings.Contains(session, "meta-50") ||
			created >= "2026-03-17" ||
			status == "TODO" || status == "REOPENED" || status == "IN_PROGRESS"
		if isSprint {
			b := 5 - daysToRelease
			if b < 0 {
				b = 0
			}
			releaseBonus = b
		}

		// broken_bonus.
		brokenBonus := 0
		combined := strings.ToLower(t.Title) + " " + strings.ToLower(t.Why)
		for _, kw := range []string{
			"не открывается", "не работает", "crash", "critical fix", "missing",
			"broken", "сломан", "corrupt", "блокирует",
		} {
			if strings.Contains(combined, kw) {
				brokenBonus = 15
				break
			}
		}

		// consumers_bonus (capped at 15).
		consumersBonus := len(t.Consumers) * 3
		if consumersBonus > 15 {
			consumersBonus = 15
		}

		base := 0.0
		if hasSpec {
			base += 5.0
		}
		base += impactPoints
		base += subjective
		base += float64(releaseBonus)
		base += float64(brokenBonus)
		base += float64(consumersBonus)
		base += float64(t.PriorityOverride)

		baseScores[tid] = baseEntry{
			base:           base,
			impactPoints:   impactPoints,
			subjective:     subjective,
			releaseBonus:   releaseBonus,
			brokenBonus:    brokenBonus,
			consumersBonus: consumersBonus,
			hasSpec:        hasSpec,
			isBlocked:      isBlocked,
			taskType:       taskType,
		}
	}

	// Step 2: bottom-up blocker_value (fixpoint, max 10 rounds, cycle-guarded).
	potentials := map[int]float64{}
	for tid, s := range baseScores {
		potentials[tid] = s.base
	}
	reach := blocksReachability(blocksMap)

	for round := 0; round < 10; round++ {
		changed := false
		for tid := range potentials {
			blockedTasks := blocksMap[tid]
			if len(blockedTasks) == 0 {
				continue
			}
			sum := 0.0
			for _, bt := range blockedTasks {
				// Skip cyclic back-edges: bt reaches tid.
				if reach[bt] != nil && reach[bt][tid] {
					continue
				}
				sum += potentials[bt]
			}
			blockerValue := sum * 0.3
			newPot := roundHalfEven(baseScores[tid].base+blockerValue, 1)
			if math.Abs(newPot-potentials[tid]) > 0.1 {
				potentials[tid] = newPot
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	// Step 3: build results (preserve input task order).
	results := make([]Result, 0, len(tasks))
	for _, t := range tasks {
		tid := t.ID
		bs := baseScores[tid]
		pot := potentials[tid]
		unblocks := len(blocksMap[tid])
		var effective float64
		if bs.isBlocked {
			effective = 0.0
		} else {
			effective = pot
		}
		results = append(results, Result{
			ID:             tid,
			Effective:      effective,
			Potential:      pot,
			Base:           bs.base,
			ImpactPoints:   bs.impactPoints,
			Subjective:     bs.subjective,
			ReleaseBonus:   bs.releaseBonus,
			BrokenBonus:    bs.brokenBonus,
			ConsumersBonus: bs.consumersBonus,
			BlockerValue:   roundHalfEven(pot-bs.base, 1),
			Unblocks:       unblocks,
			IsBlocked:      bs.isBlocked,
			HasSpec:        bs.hasSpec,
			Type:           bs.taskType,
		})
	}
	return results
}

// RecalculateUnblocked mirrors scoring.recalculate_unblocked (Python's
// entry-point for post-DONE rescore). Removes closedTaskID from BlockedBy
// of every descendant (in-place), then runs RecalculateAll and writes
// EffectiveScore + Breakdown back into each task struct. Returns IDs that
// became fully unblocked (empty BlockedBy after removal).
func RecalculateUnblocked(closedTaskID int, tasks []Task, releaseDate string, capLookup CapabilityStatusFunc) []int {
	var unblocked []int
	for i := range tasks {
		newBlocked := tasks[i].BlockedBy[:0]
		removed := false
		for _, b := range tasks[i].BlockedBy {
			if b == closedTaskID {
				removed = true
				continue
			}
			newBlocked = append(newBlocked, b)
		}
		if removed {
			tasks[i].BlockedBy = newBlocked
			if len(tasks[i].BlockedBy) == 0 {
				unblocked = append(unblocked, tasks[i].ID)
			}
		}
	}

	results := RecalculateAll(tasks, releaseDate, capLookup)
	byID := map[int]Result{}
	for _, r := range results {
		byID[r.ID] = r
	}
	for i := range tasks {
		if r, ok := byID[tasks[i].ID]; ok {
			tasks[i].EffectiveScore = r.Effective
			tasks[i].Breakdown = ScoreBreakdown{
				Base:           r.Base,
				BlockerValue:   r.BlockerValue,
				ImpactPoints:   r.ImpactPoints,
				Subjective:     r.Subjective,
				ReleaseBonus:   r.ReleaseBonus,
				BrokenBonus:    r.BrokenBonus,
				ConsumersBonus: r.ConsumersBonus,
				Potential:      r.Potential,
				Unblocks:       r.Unblocks,
				IsBlocked:      r.IsBlocked,
				HasSpec:        r.HasSpec,
				Type:           r.Type,
			}
		}
	}
	return unblocked
}
