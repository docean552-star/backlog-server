// Package client is the HTTP client mirror of internal/server. It hits the
// same five Phase 1+2 endpoints and decodes responses into the same store
// types so client subcommands and any consumers can share one set of structs.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/docean552-star/backlog-server/internal/store"
)

const defaultTimeout = 5 * time.Second

type Client struct {
	base string
	key  string
	http *http.Client
}

func New(baseURL, agentKey string) *Client {
	return &Client{
		base: baseURL,
		key:  agentKey,
		http: &http.Client{Timeout: defaultTimeout},
	}
}

// LastCacheHeader records the X-Cache value from the most recent response.
// Useful for smoke tests and the --verbose CLI flag.
func (c *Client) get(ctx context.Context, path string, dst any) (cacheHeader string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Agent-Key", c.key)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	cacheHeader = resp.Header.Get("X-Cache")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return cacheHeader, err
	}
	if resp.StatusCode >= 400 {
		return cacheHeader, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	if dst != nil {
		if err := json.Unmarshal(body, dst); err != nil {
			return cacheHeader, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return cacheHeader, nil
}

// Healthz pings the server. Returns (cacheHeader, ok, err).
func (c *Client) Healthz(ctx context.Context) (string, bool, error) {
	var body map[string]any
	cache, err := c.get(ctx, "/healthz", &body)
	if err != nil {
		return cache, false, err
	}
	ok, _ := body["ok"].(bool)
	return cache, ok, nil
}

func (c *Client) ListTasks(ctx context.Context, owner, status string) ([]store.Task, string, error) {
	q := url.Values{}
	if owner != "" {
		q.Set("owner", owner)
	}
	if status != "" {
		q.Set("status", status)
	}
	path := "/tasks"
	if e := q.Encode(); e != "" {
		path += "?" + e
	}
	var out []store.Task
	cache, err := c.get(ctx, path, &out)
	return out, cache, err
}

func (c *Client) GetTask(ctx context.Context, id int) (store.Task, string, error) {
	var out store.Task
	cache, err := c.get(ctx, "/task/"+strconv.Itoa(id), &out)
	return out, cache, err
}

func (c *Client) StatusCounts(ctx context.Context) (map[string]int, string, error) {
	var out map[string]int
	cache, err := c.get(ctx, "/status", &out)
	return out, cache, err
}

func (c *Client) NextForAgent(ctx context.Context, agent string, limit int) (store.NextResult, string, error) {
	path := "/next/" + url.PathEscape(agent)
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	var out store.NextResult
	cache, err := c.get(ctx, path, &out)
	return out, cache, err
}
