// Package notify subscribes to PostgreSQL NOTIFY events on the tasks_changed
// channel and bumps a cache version on each event. The trigger that emits the
// notifications lives in store.notifyTriggerDDL.
package notify

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/docean552-star/backlog-server/internal/cache"
)

const channel = "tasks_changed"

// Run blocks until ctx is cancelled, subscribing to the tasks_changed channel
// and calling c.Bump() on every notification. On connection errors it logs and
// retries with exponential backoff (capped at 30s). Returns nil on clean shutdown.
//
// We use a dedicated bare pgx.Conn (not the pool) because LISTEN binds to a
// specific connection and pool conns can be recycled out from under us.
func Run(ctx context.Context, dsn string, c *cache.Cache) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := listenOnce(ctx, dsn, c); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Printf("notify: listener error: %v (reconnecting in %s)", err, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		// listenOnce returned nil → ctx done; reset for safety and loop checks ctx.Err().
		backoff = time.Second
	}
}

func listenOnce(ctx context.Context, dsn string, c *cache.Cache) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	log.Printf("notify: listening on PG channel %q (cache version v%d)", channel, c.Version())

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		c.Bump()
		log.Printf("notify: %s payload=%q → cache bumped to v%d", n.Channel, n.Payload, c.Version())
	}
}
