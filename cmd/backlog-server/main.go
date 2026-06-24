package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/docean552-star/backlog-server/internal/cache"
	"github.com/docean552-star/backlog-server/internal/cli"
	"github.com/docean552-star/backlog-server/internal/config"
	"github.com/docean552-star/backlog-server/internal/notify"
	"github.com/docean552-star/backlog-server/internal/server"
	"github.com/docean552-star/backlog-server/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := runServe(); err != nil {
			log.Fatalf("serve: %v", err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		if err := cli.Run(os.Args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[1], err)
			os.Exit(1)
		}
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `backlog-server — central REST service for the backlogist DB.

Server mode:
  backlog-server serve                  Run HTTP server on $BACKLOG_HTTP_ADDR (default :8090).

Client mode (hits $BACKLOG_SERVER_URL):
  backlog-server next <agent> [--limit=N] [--json]
  backlog-server status [--json]
  backlog-server info <id> [--json]
  backlog-server tasks [--owner=] [--status=] [--json]
  backlog-server healthz

  backlog-server help                   Show this message.

Config: env vars > ./.env > $HOME/.backlog-server.env.
Server required: BACKLOG_PG_DSN, BACKLOG_AGENT_KEY.
Server optional: BACKLOG_HTTP_ADDR (default :8090), BACKLOG_REDIS_URL (enables cache + NOTIFY).
Client required: BACKLOG_SERVER_URL, BACKLOG_AGENT_KEY.
`)
}

func runServe() error {
	cfg := config.Load()
	if err := cfg.Validate(config.ModeServer); err != nil {
		return err
	}

	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()

	c, err := cache.New(initCtx, cfg.RedisURL)
	if err != nil {
		// Cache failure is non-fatal: log and run without cache rather than refusing
		// to boot. Phase 1 behaviour still works.
		log.Printf("cache: init failed, running without cache: %v", err)
		c = nil
	}
	if c != nil {
		log.Printf("cache: connected (Redis), starting at version v%d", c.Version())
	} else {
		log.Print("cache: disabled (BACKLOG_REDIS_URL empty or unreachable)")
	}

	st, err := store.New(initCtx, cfg.PGDSN, c)
	if err != nil {
		return fmt.Errorf("store init: %w", err)
	}
	defer st.Close()

	// Notify subscriber runs only if we have a cache to invalidate.
	runCtx, cancelRun := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	if c != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := notify.Run(runCtx, cfg.PGDSN, c); err != nil {
				log.Printf("notify: stopped with error: %v", err)
			}
		}()
	}

	srv := server.New(cfg, st)

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
	case err := <-errCh:
		cancelRun()
		wg.Wait()
		_ = c.Close()
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
		return nil
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	cancelRun()
	wg.Wait()
	_ = c.Close()
	log.Print("backlog-server stopped")
	return nil
}
