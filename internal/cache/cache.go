// Package cache wraps a Redis client with version-keyed entries.
//
// Invalidation strategy: a single in-memory atomic version is prefixed onto every
// cache key (bs:v{N}:<suffix>). On a NOTIFY from PostgreSQL the listener calls
// Bump(), the version increments, all future writes/reads land on the new prefix,
// and old keys age out via TTL. This avoids a SCAN+DEL stampede on noisy writes
// and keeps invalidation O(1).
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// startVersion returns a fresh per-process version prefix so cache keys from a
// previous server run cannot be served back to a new one after restart. Using
// the current UnixNano gives a strictly-growing value across restarts (clock
// drift aside) and leaves headroom of ~290 years before uint64 overflow.
func startVersion() uint64 {
	v := uint64(time.Now().UnixNano())
	if v == 0 {
		v = 1
	}
	return v
}

// DefaultTTL is the safety-net expiry on cache entries; NOTIFY-driven version
// bumps usually retire keys well before this fires.
const DefaultTTL = 5 * time.Minute

// Cache wraps a Redis client. The zero value is a no-op cache (all Get → miss,
// Set → ignored), which lets callers use a nil-or-empty *Cache as "caching off".
type Cache struct {
	rdb *redis.Client
	ver atomic.Uint64
}

// New parses a Redis URL (redis://[:pass@]host:port/db) and returns a Cache.
// Returns nil Cache (no-op) if url is empty.
func New(ctx context.Context, url string) (*Cache, error) {
	if url == "" {
		return nil, nil
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis ParseURL: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	c := &Cache{rdb: rdb}
	c.ver.Store(startVersion())
	return c, nil
}

// Close releases the underlying Redis connection.
func (c *Cache) Close() error {
	if c == nil {
		return nil
	}
	return c.rdb.Close()
}

// Bump increments the version counter so subsequent reads/writes use a new key
// prefix. Safe to call concurrently.
func (c *Cache) Bump() {
	if c == nil {
		return
	}
	c.ver.Add(1)
}

// Version returns the current key-prefix version (for tests / logging).
func (c *Cache) Version() uint64 {
	if c == nil {
		return 0
	}
	return c.ver.Load()
}

func (c *Cache) key(suffix string) string {
	return fmt.Sprintf("bs:v%d:%s", c.ver.Load(), suffix)
}

// Get fetches and JSON-decodes the cache entry for suffix into dst. Returns
// (true, nil) on hit, (false, nil) on miss, (false, err) on a real error.
func (c *Cache) Get(ctx context.Context, suffix string, dst any) (bool, error) {
	if c == nil {
		return false, nil
	}
	data, err := c.rdb.Get(ctx, c.key(suffix)).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, dst); err != nil {
		// A corrupt entry is treated as a miss so the caller refetches and overwrites.
		return false, nil
	}
	return true, nil
}

// Set JSON-encodes val and writes it under suffix with DefaultTTL.
func (c *Cache) Set(ctx context.Context, suffix string, val any) error {
	if c == nil {
		return nil
	}
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, c.key(suffix), data, DefaultTTL).Err()
}
