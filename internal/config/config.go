package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	// Server-side
	PGDSN    string
	RedisURL string
	HTTPAddr string

	// Client-side
	ServerURL string

	// Common
	AgentKey string
}

// Mode controls which fields Validate enforces.
type Mode int

const (
	ModeServer Mode = iota
	ModeClient
)

// Load reads env vars and merges values from .env (cwd) and $HOME/.backlog-server.env.
// Real env always wins; .env files only populate missing keys. Validation is left
// to Validate(mode) so the same loader serves both `serve` and client subcommands.
func Load() Config {
	loadDotenv(".env")
	if home, err := os.UserHomeDir(); err == nil {
		loadDotenv(filepath.Join(home, ".backlog-server.env"))
	}

	c := Config{
		PGDSN:     os.Getenv("BACKLOG_PG_DSN"),
		RedisURL:  os.Getenv("BACKLOG_REDIS_URL"),
		HTTPAddr:  os.Getenv("BACKLOG_HTTP_ADDR"),
		ServerURL: os.Getenv("BACKLOG_SERVER_URL"),
		AgentKey:  os.Getenv("BACKLOG_AGENT_KEY"),
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8090"
	}
	return c
}

// Validate enforces the keys required for a given run mode.
func (c Config) Validate(mode Mode) error {
	if c.AgentKey == "" {
		return errors.New("BACKLOG_AGENT_KEY is required (set in env or .env)")
	}
	switch mode {
	case ModeServer:
		if c.PGDSN == "" {
			return errors.New("BACKLOG_PG_DSN is required for serve mode")
		}
	case ModeClient:
		if c.ServerURL == "" {
			return errors.New("BACKLOG_SERVER_URL is required for client mode (e.g. https://backlog.vps:8090)")
		}
	}
	return nil
}

func loadDotenv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"'`)
		if key == "" {
			continue
		}
		if _, ok := os.LookupEnv(key); ok {
			continue
		}
		_ = os.Setenv(key, val)
	}
}
