package server

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/docean552-star/backlog-server/internal/config"
	"github.com/docean552-star/backlog-server/internal/store"
)

type Server struct {
	cfg   config.Config
	store *store.Store
	http  *http.Server
}

func New(cfg config.Config, st *store.Store) *Server {
	s := &Server{cfg: cfg, store: st}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(authMiddleware(cfg.AgentKey))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/tasks", s.handleListTasks)
	r.Get("/task/{id}", s.handleGetTask)
	r.Get("/task/{id}/history", s.handleHistory)
	r.Get("/search", s.handleSearch)
	r.Get("/analytics", s.handleAnalytics)
	r.Get("/anomalies", s.handleAnomalies)
	r.Get("/task/{id}/edges", s.handleEdges)
	r.Get("/next/{agent}", s.handleNext)
	r.Get("/status", s.handleStatus)
	r.Post("/task/{id}/advance", s.handleAdvance)
	r.Post("/task/{id}/take", s.handleTake)
	r.Post("/task/{id}/release", s.handleRelease)
	r.Post("/task/{id}/cancel", s.handleCancel)
	r.Post("/task/{id}/supersede", s.handleSupersede)
	r.Post("/task/{id}/review-submit", s.handleReviewSubmit)
	r.Post("/task/{id}/verify", s.handleVerify)
	r.Post("/task/{id}/postmortem", s.handlePostmortem)
	r.Post("/task/{id}/revision", s.handleRevision)
	r.Post("/task/{id}/freeze-update", s.handleFreezeUpdate)
	r.Post("/task/{id}/merge", s.handleMerge)
	r.Post("/task/{id}/subtasks-from-plan", s.handleSubtasksFromPlan)
	r.Get("/task/{id}/parse-recommendations", s.handleParseRecommendations)
	r.Post("/smm/trigger", s.handleSMMTrigger)
	r.Get("/smm/runs/{id}", s.handleSMMGetRun)
	r.Get("/smm/reports/{slug}/{date}", s.handleSMMGetReport)
	r.Post("/knowledge", s.handleKnowledge)
	r.Patch("/task/{id}", s.handleUpdate)
	r.Post("/exec", s.handleExec)

	s.http = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	log.Printf("backlog-server listening on %s", s.cfg.HTTPAddr)
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
