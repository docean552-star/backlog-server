package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/docean552-star/backlog-server/internal/store"
)

const requestTimeout = 10 * time.Second

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// setCache stamps X-Cache: HIT|MISS so smoke tests + ops can see the cache state.
// Must be called BEFORE writeJSON (which writes headers via WriteHeader).
func setCache(w http.ResponseWriter, hit bool) {
	if hit {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	owner := r.URL.Query().Get("owner")
	status := r.URL.Query().Get("status")
	tasks, hit, err := s.store.ListTasks(ctx, owner, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	setCache(w, hit)
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	t, hit, err := s.store.GetTask(ctx, id)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	setCache(w, hit)
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleNext(w http.ResponseWriter, r *http.Request) {
	agent := chi.URLParam(r, "agent")
	if agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent path param required"})
		return
	}
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, hit, err := s.store.NextForAgent(ctx, agent, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	setCache(w, hit)
	writeJSON(w, http.StatusOK, res)
}

// handleAdvance is the first native write endpoint (POST /task/{id}/advance).
// It runs the server-side DB gates and UPDATE + audit-trail atomically.
// Body: {agent, approve?}. `approve` is optional (default false) and only
// matters for the AWAITING_APPROVAL → DONE transition, where it maps to the
// CLI's --approve flag; other transitions ignore it. File-based gates
// (research.md content, task_plan KQ/TS count) are the client's job — the
// server never touches specs on disk. See store.AdvanceTask godoc.
func (s *Server) handleAdvance(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent   string `json:"agent"`
		Approve bool   `json:"approve"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.AdvanceTask(ctx, id, req.Agent, req.Approve)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		// 422 Unprocessable Entity — semantics fit: request was well-formed,
		// but server-side rules blocked the state change.
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleCancel — POST /task/{id}/cancel. Body: {agent, reason?}. Idempotent
// on already-terminal statuses (returns 422 TRANSITION).
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.CancelTask(ctx, id, req.Agent, req.Reason)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSupersede — POST /task/{id}/supersede. Body: {agent, by_id}.
// Differs from handleCancel in three ways: requires by_id in the body, DONE is
// a valid source (not terminal for supersede), and the replacement task must
// exist (422 REPLACEMENT if not).
func (s *Server) handleSupersede(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent string `json:"agent"`
		ByID  int    `json:"by_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if req.ByID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "by_id required (positive integer)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.SupersedeTask(ctx, id, req.Agent, req.ByID)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleReviewSubmit — POST /task/{id}/review-submit. Body:
// {agent, reviewer, verdict, summary?, is_aggregate?}.
//
//   - agent = the CLI operator (audit_trail.agent). Matches the other
//     endpoints' body convention.
//   - reviewer = the reviewer model whose verdict this records
//     (review_results.reviewer_model). Comes from the CLI's --agent flag.
//   - verdict ∈ {PASS, ACCEPT, NEEDS_WORK, FAIL, REOPEN}. ACCEPT will hit
//     the schema CHECK on review_results.verdict and 500 — this is pre-existing
//     Python behaviour that we mirror. REOPEN is remapped to NEEDS_WORK for
//     the row, kept verbatim in audit_trail.
//   - summary optional (default "{reviewer} review: {verdict}").
//   - is_aggregate optional (drives audit field switch for #981 aggregate flow).
func (s *Server) handleReviewSubmit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent       string `json:"agent"`
		Reviewer    string `json:"reviewer"`
		Verdict     string `json:"verdict"`
		Summary     string `json:"summary"`
		IsAggregate bool   `json:"is_aggregate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	req.Reviewer = strings.TrimSpace(req.Reviewer)
	req.Verdict = strings.TrimSpace(req.Verdict)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required (caller / audit_trail.agent)"})
		return
	}
	if req.Reviewer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "reviewer required (reviewer_model — e.g. code-reviewer)"})
		return
	}
	if req.Verdict == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "verdict required (PASS/ACCEPT/NEEDS_WORK/FAIL/REOPEN)"})
		return
	}
	if !store.IsValidReviewVerdict(req.Verdict) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid verdict '" + req.Verdict + "' — use PASS/ACCEPT/NEEDS_WORK/FAIL/REOPEN"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.SubmitReview(ctx, id, req.Agent, req.Reviewer, req.Verdict, req.Summary, req.IsAggregate)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleVerify — POST /task/{id}/verify. Body: {agent, failed?}. When `failed`
// is a non-empty string, the task goes back to INVESTIGATING with the reason
// appended to progress; otherwise reporter_verified is set to true. Only
// meaningful for infra_fix workflow tasks — the server does not check that.
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		Failed string `json:"failed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.VerifyTask(ctx, id, req.Agent, strings.TrimSpace(req.Failed))
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handlePostmortem — POST /task/{id}/postmortem. Body: {agent, path}. Path is
// required and stored verbatim in postmortem_path; server does not verify the
// file exists on disk (client-side warning in Python cmd_postmortem).
func (s *Server) handlePostmortem(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent string `json:"agent"`
		Path  string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	req.Path = strings.TrimSpace(req.Path)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if req.Path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.SetPostmortem(ctx, id, req.Agent, req.Path)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleRevision — POST /task/{id}/revision. Body: {agent, reason}. Reason
// required; task must be in AWAITING_APPROVAL (422 TRANSITION otherwise).
func (s *Server) handleRevision(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if req.Reason == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "reason required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.RequestRevision(ctx, id, req.Agent, req.Reason)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleEdges — GET /task/{id}/edges. Returns task_edges rows in either
// direction, ordered by (from_task_id, to_task_id). Empty array when the
// task has no edges — no 404 (this endpoint is meant to be polled).
func (s *Server) handleEdges(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	edges, err := s.store.TaskEdges(ctx, id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, edges)
}

// handleAnomalies — GET /anomalies. Returns the JSON version of Python
// cmd_anomalies (five of six anomaly types — missing-context-map stays on
// /exec because it needs a yaml file the server doesn't ship).
func (s *Server) handleAnomalies(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.Anomalies(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleAnalytics — GET /analytics?agent=&period=Nd. Returns a thin JSON
// subset of Python cmd_analytics: velocity (DONE per week over the period),
// per-status counts, and by-agent totals (Done / Active / Blocked buckets).
// Bottlenecks / time_in_status / cost sections stay on /exec.
func (s *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	agent := q.Get("agent")
	// period is "Nd" like the CLI flag; parse Ndigit prefix, default 28.
	period := 28
	if p := q.Get("period"); p != "" {
		p = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(p)), "d")
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 3650 {
			period = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.Analytics(ctx, agent, period)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSearch — GET /search?owner=&status=&text=&limit=&sort=. Simplified
// subset of Python cmd_search: no negation filters, no positional word AND
// combos, no --client/--project. The CLI dispatcher falls back to /exec when
// the caller's query shape falls outside this set.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 0
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	tasks, err := s.store.SearchTasks(ctx, q.Get("owner"), q.Get("status"), q.Get("text"), q.Get("sort"), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

// handleHistory — GET /task/{id}/history?limit=N. Returns audit_trail rows
// for the task, oldest first. Missing task → empty array (no 404), matching
// audit-feed semantics.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 5000 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	rows, err := s.store.TaskHistory(ctx, id, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

// handleKnowledge — POST /knowledge. Body:
// {task_id?, context, decision, consequences, source?}. 400 if all of
// context/decision/consequences are empty (nothing to save).
func (s *Server) handleKnowledge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TaskID       *int   `json:"task_id"`
		Context      string `json:"context"`
		Decision     string `json:"decision"`
		Consequences string `json:"consequences"`
		Source       string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Context = strings.TrimSpace(req.Context)
	req.Decision = strings.TrimSpace(req.Decision)
	req.Consequences = strings.TrimSpace(req.Consequences)
	req.Source = strings.TrimSpace(req.Source)
	if req.Context == "" && req.Decision == "" && req.Consequences == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "at least one of context/decision/consequences required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.AddKnowledge(ctx, req.TaskID, req.Context, req.Decision, req.Consequences, req.Source)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleFreezeUpdate — POST /task/{id}/freeze-update. Body: {agent, reason}.
// reason required (400); note+why both empty on the target → 422 EMPTY_INTENT.
func (s *Server) handleFreezeUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if req.Reason == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "reason required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.FreezeUpdate(ctx, id, req.Agent, req.Reason)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleUpdate — PATCH /task/{id}. Body: {agent, updates: {field: value}}.
// Supports only whitelisted text fields (see store.updatableTextFields).
// Complex updates (status transitions, custom_fields merge, JSON list columns)
// still route through /exec.
func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent   string            `json:"agent"`
		Updates map[string]string `json:"updates"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if len(req.Updates) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "updates map required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.UpdateTask(ctx, id, req.Agent, req.Updates)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleTake — POST /task/{id}/take. Sets status=IN_PROGRESS + owner=agent.
// Does not populate custom_fields.required_agents/reviews (MVP: TASKOWNERS
// registry still lives in Python; advance/IN_REVIEW gate uses the /exec path).
func (s *Server) handleTake(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent string `json:"agent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.TakeTask(ctx, id, req.Agent)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleRelease — POST /task/{id}/release. Sets status=READY, keeps owner.
func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent string `json:"agent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.ReleaseTask(ctx, id, req.Agent)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	counts, hit, err := s.store.StatusCounts(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	setCache(w, hit)
	writeJSON(w, http.StatusOK, counts)
}

// handleSubtasksFromPlan — POST /task/{id}/subtasks-from-plan.
// Body: {agent, dry_run?}. Reads parent's task_plan.md from disk, parses
// "### Phase N: Title" sections + "- [ ] item" checklists, creates one
// subtask per phase. Parent REOPENED if it was DONE.
//
// 400: malformed body / missing agent.
// 404: parent not found.
// 422: SPEC_MISSING (no task_plan file at task.task_plan or convention path).
// 200: success + { task_id, spec_path, phases[], parent_reopened, dry_run }.
func (s *Server) handleSubtasksFromPlan(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		DryRun bool   `json:"dry_run"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.SubtasksFromPlan(ctx, id, req.Agent, req.DryRun)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleMerge — POST /task/{id}/merge. Body: {absorbed_id, agent, dry_run?}.
//
// Absorbs task B (absorbed_id) into task A (path id):
//   - union of list fields on A; done_when items from B tagged "(from #B) "
//   - tasks blocked_by B → replaced with A (dedup)
//   - B.status = SUPERSEDED, note = "Merged into #A"
//   - audit_trail per changed field on A + one for B's status change
//
// 400: malformed body / missing absorbed_id / missing agent.
// 404: A or B not found.
// 422: SAME_TASK / TARGET_TERMINAL / ABSORBED_TERMINAL.
// 200: success + { task_a, fields_updated, redirected_dep_ids, b_status, dry_run }.
func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		AbsorbedID int    `json:"absorbed_id"`
		Agent      string `json:"agent"`
		DryRun     bool   `json:"dry_run"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	if req.AbsorbedID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "absorbed_id required (positive integer)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.MergeTasks(ctx, id, req.AbsorbedID, req.Agent, req.DryRun)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(res.Failures) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleParseRecommendations — GET /task/{id}/parse-recommendations.
//
// Reads parent's review-result.md, extracts the yaml block, validates
// mandatory fields, and returns recommendations + ready-to-run
// `backlogist create` commands. Non-fatal parse failures (missing file,
// missing section, malformed yaml, per-item missing fields) return 200
// with errors[] populated — parity with Python's stdout-only reporting.
//
// 200: success (recommendations may be empty).
// 400: id not integer.
// 404: task not found.
// 500: DB error / read error other than missing file.
func (s *Server) handleParseRecommendations(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.ParseRecommendations(ctx, id)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSMMTrigger — POST /smm/trigger. Body: {job?, agent?}.
//
// job defaults to "pipeline" (full 5-step smm-daily-monitor.sh). MVP scope
// covers only "pipeline"; "content_agent" (step-5-only) is deferred.
// agent defaults to "unknown" if the client doesn't send one.
//
// 202: {run_id, job, status:"queued"}. Client polls GET /smm/runs/{run_id}.
// 400: unsupported job value.
// 500: mkdir/spawn failure (rare — filesystem or wrapper missing).
func (s *Server) handleSMMTrigger(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Job       string `json:"job"`
		Agent     string `json:"agent"`
		Client    string `json:"client"`
		SocialSet string `json:"social_set"`
		Count     string `json:"count"`
	}
	// Body is optional — an empty POST is valid and triggers a pipeline run.
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
	}
	req.Job = strings.TrimSpace(req.Job)
	req.Agent = strings.TrimSpace(req.Agent)
	req.Client = strings.TrimSpace(req.Client)
	req.SocialSet = strings.TrimSpace(req.SocialSet)
	req.Count = strings.TrimSpace(req.Count)
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.TriggerSMM(ctx, req.Job, req.Agent, store.TriggerSMMParams{
		Client: req.Client, SocialSet: req.SocialSet, Count: req.Count,
	})
	if err != nil {
		msg := err.Error()
		if strings.HasPrefix(msg, "job must be") || strings.HasPrefix(msg, "params must match") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": msg})
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

// handleSMMGetRun — GET /smm/runs/{id}.
//
// Reads wrapper-maintained state file at /opt/apps/ax/runs/{id}.json.
// Populates log_tail (last ~50 lines of run log). Watchdogs zombies:
// running with a dead PID → auto-marked failed with exit_code=-1.
func (s *Server) handleSMMGetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	if runID == "" || strings.ContainsAny(runID, "./\\") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid run_id"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.GetSMMRun(ctx, runID)
	if err != nil {
		if err == store.ErrRunNotFound {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "run_id not found", "run_id": runID})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSMMGetReport — GET /smm/reports/{slug}/{date}.
//
// Thin file server for /opt/apps/ax/output/smm/clients/{slug}/reports/{date}.json.
// Slug and date are sanitised against ../ and /. Response is raw JSON.
func (s *Server) handleSMMGetReport(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "slug")
	date := chi.URLParam(r, "date")
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	data, err := s.store.ReadSMMReport(ctx, slug, date)
	if err != nil {
		if err == store.ErrRunNotFound {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "report not found", "slug": slug, "date": date})
			return
		}
		if strings.HasPrefix(err.Error(), "invalid slug") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleAggregateReview — POST /task/{id}/aggregate-review (#1392).
//
// Body: {agent, force?, dry_run?}. Runs the server-portable slice of Python
// cmd_aggregate_review: verify parent (has_child), load workflow config,
// aggregate children cost/count, and either auto-record PASS verdict (trivial
// cluster) or hand back check-names for the client to run locally.
//
// 200 skipped=true:  server wrote paired audit_trail + review_results rows;
//                    sync_aggregate_on_verdict_trigger fired -> aggregate_state
//                    -> passed. Client just prints the auto-skip message.
// 200 skipped=false: non-trivial cluster / --force / --dry-run — client continues
//                    with local cheap_checks + evidence + LLM prompt.
// 400: agent missing / invalid JSON / bad id.
// 404: task not found.
func (s *Server) handleAggregateReview(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id must be integer"})
		return
	}
	var req struct {
		Agent  string `json:"agent"`
		Force  bool   `json:"force"`
		DryRun bool   `json:"dry_run"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.BeginAggregateReview(ctx, id, req.Agent, req.Force, req.DryRun)
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSessionClose — POST /session/close (#1390).
//
// Body: {agent, session_label, done_ids[]}. Writes one audit_trail row per
// existing DONE task with field_changed='session_close', new_value=session_label,
// command='session-close'. IDs missing from tasks table come back under
// skipped_ids for the client to surface (non-fatal — HANDOFF may reference
// hard-deleted tasks). Empty done_ids returns 200 with inserted=0 (think-only
// session — the git commit itself is the session record).
//
// 200: {session_label, inserted, skipped_ids[], timestamp}.
// 400: agent missing / invalid body.
// 500: DB failure mid-transaction.
func (s *Server) handleSessionClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Agent        string `json:"agent"`
		SessionLabel string `json:"session_label"`
		DoneIDs      []int  `json:"done_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Agent = strings.TrimSpace(req.Agent)
	req.SessionLabel = strings.TrimSpace(req.SessionLabel)
	if req.Agent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := s.store.RecordSessionClose(ctx, req.Agent, req.SessionLabel, req.DoneIDs)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}
