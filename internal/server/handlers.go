package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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
