# backlog-server — DESIGN & BUILD SPEC

> Self-contained spec. A cold session can build Phase 1 straight from this file.
> Authored in samvel-30 architecture session (2026-06-23). Agent: Samvel (Go backend).
> Status: ARCHITECTURE LOCKED. Phase 1 not yet coded (awaiting "Кодь").

---

## 1. Why this exists (the problem)

`backlogist` (Python CLI in `ax/backlogist/`) is the project's task manager. It has **no server**: every
CLI invocation AND every Claude Code hook does a **cold Python start + a fresh `psycopg2.connect()` to a
REMOTE PostgreSQL over the internet**, then several queries, then dies.

Measured this session (when DB healthy): ping VPS ~13ms, connect ~70ms, each query ~25ms RTT.
A single command = connect (70ms) + N queries × 25ms. Hooks fire **4–5× per tool-call** in a session
(PreToolUse + PostToolUse groups), each a separate cold process + remote connect. Result: the whole
session feels slow ("ооочень медленно"), and under parallel bursts the remote PG's 100-conn limit gets
exhausted / leaks idle-in-transaction → multi-second hangs (meta reported this).

**Root cause: architecture, not a missing feature.** It is NOT a graph problem (operator killed the
#1199 graph-engine direction as over-engineering for a task tracker — see §8).

## 2. The fix (locked decisions)

Build **`backlog-server`**: a **central Go REST service hosted on the VPS**, co-located with PG + Redis.
Clients (hooks / CLI / agents, on any OS) make one HTTP call; the server does all DB/cache work over
**localhost** (warm pool + cache). Collapses "connect 70ms + N×25ms over internet" into **one ~13ms RTT**
to a warm server.

Locked operator decisions (this session):
- **Name:** `backlog-server`. New repo at `/Users/sam/Documents/Ayant-X/backlog-server/` (sibling of ax/, AyantXAPI/, AyantXFront/).
- **Language:** Go. **End-state:** Go becomes the new backlogist core; **Python retires incrementally** (not a big-bang rewrite).
- **Topology:** **CENTRAL service on the VPS**, NOT a per-machine local daemon. (Team is Windows + Mac + Linux → per-machine daemon install = launchd/systemd/Windows-Service hell. Rejected.)
- **Separate from AyantXAPI:** own repo, own port, own deploy, own auth. NOT bolted into the Go platform API.
- **Transport:** plain **HTTP/JSON REST** on a TCP port + token auth. **No WebSocket** (rejected: one-shot hook/CLI processes can't hold a persistent conn, so WS gives no speedup for them). **No unix socket** (clients are remote).
- **Cache:** **Redis** (the existing VPS Redis). Co-located with the server → server↔Redis is localhost (~0ms). Optional L1 in-memory on the server. Single central server ⇒ cache is inherently shared across all machines, no cross-machine staleness problem.
- **Cache invalidation:** PG **LISTEN/NOTIFY** (catches every write incl. direct psql / Python during transition) → server refreshes Redis/L1.
- **Config:** `.env` file (gitignored), priority env > .env > defaults (mirrors backlogist `core/config.py`).
- **Release:** `goreleaser` + GitHub Actions on git tag → prebuilt binaries (darwin_arm64, darwin_amd64, linux_amd64) as GitHub Release assets. Private repo → download via `gh release download` (anonymous curl 404s on private). **install.sh reinstall** for updates — **no self-update** in the binary.
- **Service:** systemd unit on the VPS (ONE machine). launchd/Windows-service NOT needed (no per-dev install).
- **No backlog task** for this work — operator declared this a special/unique session (ironic: we're replacing backlogist, so tracking it in backlogist makes no sense).

## 3. Project infrastructure (verified this session)

Sibling repos in `/Users/sam/Documents/Ayant-X/`:
- `ax/` — docs, backlog, specs, HANDOFF, **backlogist** (Python CLI). Command center. Has `.env` (gitignored; `.env.*` too, `!.env.example` kept).
- `AyantXAPI/` (branch d1) — Go REST API (Gin, JWT). Deploys to api-d1.
- `AyantXFront/` (branch d1) — Angular. Deploys to d1.ayant-x.com.
- `wwwayant-xcom/` — marketing site.
- `backlog-server/` — THIS project (new).

VPS **`82.24.174.216`** (everything co-located here):
- **PostgreSQL :5432** — db `backlogist`, 1244 task rows, 17MB. Creds: user `postgres` / pass `HRYycECHP2Qa` / `sslmode=disable`. This is what backlogist hits directly today.
- **Redis :6379** — pager (Redis Streams). Pass `AX_PAGER_2026`. Pager uses db 0; **backlog-server should use a different db index (e.g. /1)** to avoid key collision.
- Stands: d1 (operator), d2 (Timur), api-d1 (Go API).

backlogist DB access lives in **`ax/backlogist/storage/db.py`** — `get_db()` / `DBConnection` is the single chokepoint. `_get_pg_config()` there reads `BACKLOGIST_PG_DSN` > `BACKLOGIST_PG_*` vars > hardcoded defaults (the VPS creds above). When we route Python through backlog-server, this is the one file to swap.

Hooks: `ax/.claude/settings.json` (PreToolUse ×1, PostToolUse ×3) and `AyantXAPI/.claude/settings.json` (PreToolUse ×3, PostToolUse ×2) — these are the high-frequency callers to make fast.

Multi-agent: A, B, T1, T2, meta, samvel, angel, seo, smm — across Win/Mac/Linux, all hit the one remote PG.

## 4. Architecture

```
hooks / CLI / agents (Win/Mac/Linux)
        │  HTTP/JSON + X-Agent-Key   (ONE network RTT ~13ms)
        ▼
  backlog-server (Go, ON THE VPS, separate from AyantXAPI)
        ├─ pgxpool  → PG  on localhost:5432   (~0ms, warm pool)
        ├─ Redis    → cache on localhost:6379/1 (~0ms, co-located)
        └─ PG LISTEN/NOTIFY → cache invalidation
```

Why fast: client pays 1 RTT; server does all queries against localhost PG/Redis (warm). Hook cascade
4–5×/click: was hundreds of ms–seconds → ~50–65ms.

## 5. PHASE PLAN

- **Phase 1 (next session):** working REST server + warm pgxpool + token auth + core read endpoints + `.env`. Removes per-call connect cost. (Detailed in §6.)
- **Phase 2:** Redis cache + PG LISTEN/NOTIFY invalidation. Removes per-query RTT → hooks become instant.
- **Phase 3:** port hot commands to native Go; point hooks at `backlog-server` instead of Python; route Python `storage/db.py` through the server. Python kept only for cold/heavy commands (prepare/create/advance) during transition.
- **Deploy track (parallel):** goreleaser + GitHub Actions, systemd unit on VPS, `install.sh`.

## 6. PHASE 1 — DETAILED BUILD SPEC

Goal: a runnable central REST server with a warm PG pool + auth. Once a client points at it, the
70ms-connect-per-call cost is gone.

### 6.1 Repo skeleton
```
backlog-server/
  go.mod                          # module e.g. github.com/ayant-x/backlog-server (confirm owner)
  cmd/backlog-server/main.go      # entry: `serve` subcommand (client subcommands come later)
  internal/server/server.go       # http.Server, router, middleware wiring
  internal/server/handlers.go     # the read handlers
  internal/server/auth.go         # X-Agent-Key middleware
  internal/store/store.go         # pgxpool init + queries
  internal/config/config.go       # load env > .env > defaults
  .env                            # gitignored, real secrets (see §6.5)
  .env.example                    # committed, no secrets
  .gitignore                      # .env, .env.*, !.env.example, bin/, dist/
  DESIGN.md                       # this file
  README.md                       # quickstart
```
- `go mod init`, `git init`. Use **pgx v5** (`github.com/jackc/pgx/v5/pgxpool`). Std `net/http` + a light router (chi `github.com/go-chi/chi/v5`) or stdlib mux — keep deps minimal.
- HARD RULE: commit only explicit paths; never `git add -A` (shared parent dir tree). git init in the new repo is fine; do NOT touch sibling repos.

### 6.2 `serve` command
`backlog-server serve` → load config → init pgxpool → start `http.Server` on `BACKLOG_HTTP_ADDR` (default `:8090`).
Graceful shutdown on SIGINT/SIGTERM (close pool).

### 6.3 Auth
Middleware checks header `X-Agent-Key` == `BACKLOG_AGENT_KEY`. Missing/wrong → 401. `/healthz` is exempt.
(Pattern mirrors AyantXAPI `internal/middleware/agent_auth.go`.)

### 6.4 Endpoints (Phase 1) — all read, served from PG via warm pool
- `GET /healthz` → 200 `{"ok":true}` (no auth; also pings pool).
- `GET /tasks?owner=&status=` → JSON array of tasks (filterable).
- `GET /task/{id}` → one task (404 if absent).
- `GET /next/{agent}` → same ranking as `backlogist next <agent>` (top-N by effective_score + readiness). NOTE: replicate the ranking SQL/logic from `ax/backlogist/` (find the `next` command impl in `core/commands.py` / wherever) — match its output so it's a drop-in.
- `GET /status` → counts by status (like `backlogist status`).

Keep JSON field names matching what Python/clients expect (snake_case, mirror the `tasks` table columns). Inspect the `tasks` table schema on the VPS PG first (`\d tasks`) — 1244 rows, columns include id, title, status, owner, mode, effective_score, blocked_by, parent_id, references, done_when, why, business_value, note, etc. (confirm exact columns at build time).

### 6.5 .env (gitignored)
```
BACKLOG_PG_DSN=postgres://postgres:HRYycECHP2Qa@82.24.174.216:5432/backlogist?sslmode=disable
BACKLOG_REDIS_URL=redis://:AX_PAGER_2026@82.24.174.216:6379/1
BACKLOG_HTTP_ADDR=:8090
BACKLOG_AGENT_KEY=<generate-a-secret>
```
`.env.example` = same keys, values blanked/placeholder. On the VPS deploy, DSN host → `localhost`/`127.0.0.1`
and Redis → `localhost`. From a dev machine during testing, `.env` points at the VPS (server URL = VPS:8090).

Config loader: env var > `.env` (setdefault, never override real env) > hardcoded default. Mirror
`ax/backlogist/core/config.py` `_load_dotenv` behavior.

### 6.6 Phase 1 acceptance (smoke)
- `backlog-server serve` boots, connects pool, logs listening addr.
- `curl localhost:8090/healthz` → `{"ok":true}`.
- `curl -H "X-Agent-Key: <key>" localhost:8090/next/samvel` returns the same task ranking as
  `cd ax && python3 backlogist/backlogist.py next samvel`.
- Missing/bad key → 401.
- No regression to backlogist itself (we haven't swapped Python's db layer yet — that's Phase 3).

## 7. Open items to resolve at build time
- **go module path / GitHub owner** — confirm the repo owner/org for `go mod init` + goreleaser (operator uses GitHub, `gh` CLI). Repo is private.
- **Port `:8090`** — proposed default; confirm no conflict on VPS.
- **`next` ranking parity** — read the actual backlogist `next` implementation and the `tasks` schema before writing the query, to match output exactly.
- **VPS deploy specifics** — how the server is exposed (direct port vs reverse proxy), firewall for :8090, systemd unit path. Deferred to deploy track.

## 8. Context: why NOT the graph engine (#1199)
Operator (this session) rejected building a bespoke task-graph engine (#1199 bundle: #1033/#1034/#1035/#1036/#1038/#1148).
A task tracker already IS a small dependency graph (blocked_by, parent/child, typed edges #1025 done).
The heavy items (metamorphic testing, runtime invariants, durable execution, perf at thousands of nodes)
are workflow-engine territory (Template/Temporal-class) = YAGNI for a tracker with ~hundreds of tasks.
The REAL pain was backlogist infra latency → this `backlog-server` project. #1199 was advanced to PLANNING
during a prepare attempt (creating `ax/docs/specs/graphinfra-cluster-needs-audit-thirdpart-1199/` +
context_bundle.md) then PAUSED on operator "стой/отмена". **TODO if desired:** revert #1199 to BACKLOG and
remove that spec dir, or leave parked. Nothing committed for it.

## 9. State at end of samvel-30 session
- Onboarding done; sibling repos synced (ax pulled; AyantXFront/AyantXAPI clean).
- #1147 DONE (A closed 06-21), #1145/#368/#1055 DONE. #526 keystone now READY owner samvel (needs 7 KQ) — parked, not started.
- #737 dangling IN_PROGRESS (taken 06-20). #1199 in PLANNING (paused, see §8).
- Pager backlog: prod bug #1202 (Cloudflare 403 to AI crawlers, owner samvel), A's #1205 (seed content_decay signal on d1 for e2e). Not actioned.
- This session pivoted entirely to designing `backlog-server`. No code written. `backlog-server/` folder created with this DESIGN.md only.

## 10. NEXT SESSION — start here
1. Read this DESIGN.md fully.
2. Confirm §7 open items with operator (go module path, port).
3. Inspect VPS `tasks` table schema + backlogist `next` ranking logic.
4. Build Phase 1 per §6. Then smoke (§6.6).
5. HARD RULES still apply: commit only explicit paths (never `git add -A`); read every subagent artifact before use; shared test DB (#1066) → no parallel pytest.
