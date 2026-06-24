# backlog-server

Central Go REST service co-located with PostgreSQL + Redis on the VPS. Replaces per-call cold Python connect (~70ms + N×25ms over internet) with one warm HTTP RTT (~13ms) for backlogist clients (hooks, CLI, agents).

See `DESIGN.md` for full spec. This README covers Phase 1 quickstart only.

## Phase 1 + Phase 2 status

Working REST server with:
- Warm `pgxpool` + token auth + core read endpoints (`/healthz`, `/tasks`, `/task/{id}`, `/next/{agent}`, `/status`).
- **Redis read-through cache** (optional; co-located on the VPS). Each response carries `X-Cache: HIT|MISS`. Sequential `/next` from Mac→VPS: ~17ms cached vs ~140ms uncached. 20 concurrent: 133ms wall (vs Python cold start: 1566ms — 11.8× faster).
- **PG LISTEN/NOTIFY** invalidation. A `tasks_notify` trigger on the `tasks` table fires `pg_notify('tasks_changed', id)` on every INSERT/UPDATE/DELETE; the server bumps an atomic version that prefixes all cache keys, so stale entries become unreachable instantly and age out via TTL (5 min safety net).
- Cache is **optional**: with `BACKLOG_REDIS_URL` empty the server boots cleanly without cache or notify subscriber (Phase 1 mode).

No deploy automation yet (parallel deploy track).

## Quickstart (local)

```sh
cp .env.example .env
# Edit .env: paste real BACKLOG_PG_DSN (VPS), generate BACKLOG_AGENT_KEY (any random string).
go mod tidy
go run ./cmd/backlog-server serve
```

Server listens on `:8090` by default. Pool connects to the DSN in `.env`.

## Smoke test

```sh
# 1. Health (no auth)
curl -s localhost:8090/healthz
# {"ok":true}

# 2. Missing key -> 401
curl -s -o /dev/null -w "%{http_code}\n" localhost:8090/status

# 3. Authed reads
KEY=$(grep BACKLOG_AGENT_KEY .env | cut -d= -f2)
curl -s -H "X-Agent-Key: $KEY" localhost:8090/status
curl -s -H "X-Agent-Key: $KEY" "localhost:8090/tasks?owner=samvel&status=READY"
curl -s -H "X-Agent-Key: $KEY" localhost:8090/task/526
curl -s -H "X-Agent-Key: $KEY" localhost:8090/next/samvel
```

Parity check vs Python:

```sh
cd ../ax
python3 backlogist/backlogist.py next samvel
# Compare task IDs in the top-5 with /next/samvel JSON output.
# Expected: same ranking modulo the FS-check approximation (see DESIGN.md §6.4 + open items).
```

## Config

Loader order: real env var > `.env` file > hardcoded default.

| Key | Default | Purpose |
|---|---|---|
| `BACKLOG_PG_DSN` | (none, required) | PostgreSQL DSN for the backlogist DB. |
| `BACKLOG_REDIS_URL` | (empty → cache off) | Redis URL like `redis://:pass@host:6379/1`. Enables cache + LISTEN/NOTIFY subscriber. |
| `BACKLOG_HTTP_ADDR` | `:8090` | HTTP listen addr. |
| `BACKLOG_AGENT_KEY` | (none, required) | Required header value for `X-Agent-Key`. |

## Auth

Every endpoint except `/healthz` requires header `X-Agent-Key: <BACKLOG_AGENT_KEY>`. Missing/wrong → 401.

## `/next` ranking — known deviation from Python

Python `recommend_next` (`ax/backlogist/core/recommendations.py:23`) computes `readiness` partially from filesystem checks: it adds +0.3 if `task_plan` file exists on disk, +0.2 if `spec` file exists. The server runs on the VPS and cannot see clients' local `ax/` checkouts, so it uses **field-non-empty as a proxy**: `task_plan != ''` → +0.3, `spec != ''` → +0.2. Match in practice is ~99% because paths are written to the DB at the same `advance` step that creates the file.
