# Tenant Provisioning Control Plane: Implementation Plan

Python 3.12 · FastAPI · PostgreSQL · RabbitMQ · Docker-based development

---

## 1. Scope and priorities

Priority order (matches what the reviewers say they grade):

1. State machine correctness (tenant and task)
2. Concurrency correctness (optimistic locking, unique slug)
3. Messaging correctness (outbox, idempotent and order-tolerant consumer, retries, poison messages)
4. Tests that prove 1-3
5. Docs, quality gates, clean commit history

Explicitly out of scope: auth, metrics dashboards, Kubernetes, multi-region, a real provisioning backend.

---

## 2. Decisions at a glance

| Topic | Decision | Why |
|---|---|---|
| Language / framework | Python 3.12, FastAPI, Pydantic v2 | Auto OpenAPI, validation, async |
| DB | PostgreSQL 16 | Row locks, unique constraints, partial indexes, `SKIP LOCKED` |
| DB access | SQLAlchemy 2.0 async + asyncpg, Alembic migrations | Standard, explainable |
| Broker | RabbitMQ 3.13 (`aio-pika`) | Acks, DLX/DLQ, management UI and HTTP API for manual publishing |
| Outbound reliability | Transactional outbox + polling publisher | Never publishes before commit or on rollback |
| Inbound reliability | `processed_updates` table + transition guard | Idempotent and order-tolerant |
| Locking | Conditional `UPDATE ... WHERE version = :v AND status = ...` | Atomic, no explicit locks needed on the API path |
| Worker | Separate process, same image, `python -m cp.worker` | Matches "separate process" requirement |
| Tests | `pytest` + `pytest-asyncio` + `httpx` against real Postgres and RabbitMQ | Real concurrency, no mocks for the hard parts |
| Quality | `ruff` (lint + format), `mypy`, `bandit`, `pip-audit`, `pytest` | One command: `uv run python scripts/check.py` |
| Runner | Cross-platform Python quality runner; Podman Compose boots services | Copy-pasteable on Windows and POSIX |
| Packaging | `uv` + `pyproject.toml` + lockfile | Fast, reproducible |
| Not using | Celery, ORMs with auto-flush magic, testcontainers | See notes in section 12 |

---

## 3. Architecture

```mermaid
flowchart LR
    Client([HTTP client]) --> API[Control Plane API<br/>FastAPI]
    API -->|"1 txn: tenant + task + outbox"| PG[(PostgreSQL)]
    PUB[Outbox publisher<br/>background loop] -->|"read unpublished<br/>FOR UPDATE SKIP LOCKED"| PG
    PUB -->|"publish + confirm"| X1{{"exchange cp.tasks"}}
    X1 --> Q1[[queue cp.tasks.worker]]
    Q1 --> W[Worker simulator<br/>separate process]
    W -->|"in_progress, done, failed"| X2{{"exchange cp.task-updates"}}
    X2 --> Q2[[queue cp.task-updates]]
    Q2 --> CON[Update consumer<br/>background loop]
    CON -->|"1 txn: dedupe + task + tenant"| PG
    Q2 -. "poison / retries exhausted" .-> DLQ[[cp.task-updates.dlq]]
```

Processes (all from one image):

| Process | Command | Contains |
|---|---|---|
| `api` | `uvicorn cp.main:create_app --factory` | HTTP API + outbox publisher + update consumer (background tasks in lifespan) |
| `worker` | `python -m cp.worker --fail-rate 0 ...` | Worker simulator |

Optional later: split publisher and consumer into their own process (`python -m cp.relay`). The code is written so each loop is an independent coroutine, so this is a small change.

---

## 4. Repo layout

```
tenant-control-plane/
├── pyproject.toml
├── uv.lock
├── Makefile
├── Dockerfile
├── .dockerignore
├── .gitignore
├── .env.example
├── alembic.ini
├── migrations/
│   └── versions/0001_initial.py
├── openapi.yaml                 # hand-written contract (this repo's source of truth)
├── README.md
├── docs/
│   ├── architecture.md
│   ├── messaging.md
│   └── adr/                     # short decision records (optional)
├── scripts/
│   ├── load_test.py             # race-condition demo
│   ├── publish_update.py        # inject duplicate / out-of-order updates
│   └── replay_dlq.py
├── src/cp/
│   ├── config.py                # pydantic-settings
│   ├── logging.py
│   ├── main.py                  # create_app(), lifespan starts/stops loops
│   ├── api/
│   │   ├── routers/tenants.py
│   │   ├── routers/tasks.py
│   │   ├── routers/health.py
│   │   ├── schemas.py           # request/response models
│   │   ├── errors.py            # exception handlers -> error body
│   │   └── pagination.py        # cursor encode/decode
│   ├── domain/
│   │   ├── enums.py
│   │   ├── state_machine.py     # pure functions, no I/O
│   │   └── errors.py            # DomainError subclasses with `code`
│   ├── db/
│   │   ├── models.py
│   │   ├── session.py
│   │   └── repositories.py
│   ├── services/
│   │   ├── tenants.py           # create / patch / delete use-cases
│   │   └── task_updates.py      # apply worker update (idempotent)
│   ├── messaging/
│   │   ├── contracts.py         # pydantic models for both directions
│   │   ├── topology.py          # exchanges, queues, DLX declarations
│   │   ├── connection.py        # robust connect with backoff
│   │   ├── publisher.py         # outbox relay loop
│   │   └── consumer.py          # update consumer loop
│   └── worker/
│       ├── __main__.py          # CLI entry (argparse/typer)
│       └── simulator.py
└── tests/
    ├── unit/
    ├── integration/
    └── conftest.py
```

Layering rule: `api` → `services` → `db` / `domain`. `domain` imports nothing from the other layers. Messaging calls `services`, never touches SQL directly.

---

## 5. Data model

```sql
CREATE TYPE tenant_status AS ENUM
  ('provisioning','active','updating','destroying','destroyed','failed');
CREATE TYPE task_type   AS ENUM ('deploy','update','destroy');
CREATE TYPE task_status AS ENUM ('accepted','in_progress','done','failed');

CREATE TABLE tenants (
  id          uuid PRIMARY KEY,
  slug        varchar(28) NOT NULL,
  name        text NOT NULL CHECK (length(btrim(name)) > 0),
  status      tenant_status NOT NULL,
  version     integer NOT NULL DEFAULT 1,
  created_at  timestamptz NOT NULL,
  updated_at  timestamptz NOT NULL,
  CONSTRAINT tenants_slug_key UNIQUE (slug),           -- includes destroyed tenants
  CONSTRAINT tenants_slug_format
    CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$')
);

CREATE TABLE tasks (
  id          uuid PRIMARY KEY,                        -- also the event id
  type        task_type NOT NULL,
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  status      task_status NOT NULL,
  error       text,
  created_at  timestamptz NOT NULL,
  updated_at  timestamptz NOT NULL
);
CREATE INDEX tasks_tenant_created_idx ON tasks (tenant_id, created_at DESC, id DESC);
CREATE INDEX tasks_created_idx        ON tasks (created_at DESC, id DESC);
-- Safety net only. The real guarantee comes from the tenant status guards.
CREATE UNIQUE INDEX tasks_one_open_per_tenant
  ON tasks (tenant_id) WHERE status IN ('accepted','in_progress');

CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,                 -- publish order
  task_id       uuid NOT NULL REFERENCES tasks(id),
  routing_key   text NOT NULL,                         -- e.g. task.deploy
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_updates (
  update_id     uuid PRIMARY KEY,                      -- idempotency key
  task_id       uuid NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now()
);
```

Notes:

- Timestamps are stored as `timestamptz` (microseconds). The API serializes them as ISO 8601 with **milliseconds and `Z`** via a single custom serializer (`2026-10-11T10:15:30.123Z`). Truncate, don't round.
- IDs are UUIDv4 (server-generated).
- `tasks.error` is an additive optional field (failure reason from the worker).

---

## 6. State machines (domain layer)

Pure, table-driven, and exhaustively unit-tested.

```python
# domain/state_machine.py (shape, not final code)
TENANT_ACTION_GUARDS = {
    # (action) -> (allowed_from, to_status, task_type)
    "patch":  ({ACTIVE},            UPDATING,   UPDATE),
    "delete": ({ACTIVE, FAILED},    DESTROYING, DESTROY),
}

TASK_TRANSITIONS = {
    ACCEPTED:    {IN_PROGRESS},
    IN_PROGRESS: {DONE, FAILED},
    DONE:        set(),
    FAILED:      set(),
}

# terminal task outcome -> (expected tenant status, new tenant status)
TENANT_ON_TASK_OUTCOME = {
    (DEPLOY,  DONE):   (PROVISIONING, ACTIVE),
    (UPDATE,  DONE):   (UPDATING,     ACTIVE),
    (DESTROY, DONE):   (DESTROYING,   DESTROYED),
    (DEPLOY,  FAILED): (PROVISIONING, FAILED),
    (UPDATE,  FAILED): (UPDATING,     FAILED),
    (DESTROY, FAILED): (DESTROYING,   FAILED),
}
```

`destroyed` is terminal. No transitions out. `DELETE` on `destroyed` returns `tenant_update_not_allowed`.

---

## 7. HTTP API design

Full contract: **`openapi.yaml`**.

### Routes

| Method | Path | Success | Purpose |
|---|---|---|---|
| POST | `/v1/tenants` | 201 + `Location: /v1/tenants/{id}` | Create tenant and `deploy` task |
| GET | `/v1/tenants` | 200 | List (filter `status`, cursor pagination) |
| GET | `/v1/tenants/{tenant_id}` | 200 | Get one |
| PATCH | `/v1/tenants/{tenant_id}` | 202 + `Location: /v1/tasks/{task_id}` | Partial update, creates `update` task |
| DELETE | `/v1/tenants/{tenant_id}` | 202 + `Location: /v1/tasks/{task_id}` | Creates `destroy` task |
| GET | `/v1/tenants/{tenant_id}/tasks` | 200 | Tasks of one tenant |
| GET | `/v1/tasks` | 200 | List (filters `tenant_id`, `status`, `type`) |
| GET | `/v1/tasks/{task_id}` | 200 | Get one |
| GET | `/healthz`, `/readyz` | 200 / 503 | Liveness / readiness (DB + broker) |

Why 202 for PATCH and DELETE: the request is accepted, the work completes later. POST returns 201 because the tenant resource exists immediately; the response also carries the deploy task so the client can track it.

### Error body and status mapping

```json
{ "error": { "code": "tenant_version_conflict", "message": "...", "details": { "expected_version": 3, "current_version": 4 } } }
```

| Code | HTTP | When |
|---|---|---|
| `tenant_not_found` | 404 | Unknown tenant id |
| `task_not_found` | 404 | Unknown task id |
| `tenant_already_exists` | 409 | Slug already used (including destroyed tenants) |
| `tenant_version_conflict` | 409 | PATCH `version` does not match current |
| `tenant_update_not_allowed` | 409 | Action not allowed in the tenant's current status (PATCH or DELETE) |
| `validation_error` | 422 | Bad slug, empty name, bad cursor, bad filter, empty PATCH |
| `internal_error` | 500 | Anything unhandled (no stack trace in body) |

Override FastAPI's default 422 body with a handler so every error has the same shape.

**Check precedence on PATCH:** not found → version conflict → status guard. A stale version means the client's view is out of date, which is more useful to report first.

### Atomic writes (the core of concurrency correctness)

Create:

```sql
INSERT INTO tenants (...) VALUES (...);   -- unique violation on slug => tenant_already_exists
```

No "check then insert". Catch `IntegrityError` for `tenants_slug_key`.

PATCH (single statement is the lock):

```sql
UPDATE tenants
   SET name = COALESCE(:name, name),
       status = 'updating',
       version = version + 1,
       updated_at = :now
 WHERE id = :id AND version = :version AND status = 'active'
RETURNING *;
```

- Row returned: insert the `update` task and the `outbox` row in the same transaction, commit.
- No row: `SELECT` the tenant to classify the failure (404 / version conflict / not allowed). This second read only picks the error message; the correctness decision was already made atomically.

DELETE:

```sql
UPDATE tenants
   SET status = 'destroying', version = version + 1, updated_at = :now
 WHERE id = :id AND status IN ('active','failed')
RETURNING *;
```

### Pagination

Keyset (cursor) pagination: `limit` (1-100, default 20) and opaque `cursor` (base64 of `created_at,id`). Order: `created_at DESC, id DESC`. Response: `{ "items": [...], "next_cursor": "..." | null }`.

---

## 8. Messaging design

### Topology (declared idempotently at startup by `topology.py`)

| Object | Name | Notes |
|---|---|---|
| Exchange (direct, durable) | `cp.tasks` | Control plane → worker |
| Queue (durable) | `cp.tasks.worker` | Bound with `task.*` |
| Exchange (direct, durable) | `cp.task-updates` | Worker → control plane |
| Queue (durable) | `cp.task-updates` | Bound with `task.update`; `x-dead-letter-exchange=cp.dlx`, `x-dead-letter-routing-key=task-updates.dead` |
| Exchange (direct, durable) | `cp.dlx` | Dead letters |
| Queue (durable) | `cp.task-updates.dlq` | Bound to `cp.dlx` with `task-updates.dead` |

Messages are persistent (`delivery_mode=2`), `message_id` is set, and content type is `application/json`.

### Outbound message (task = event)

```json
{
  "id": "6b0f...",               // task id (== event id)
  "type": "deploy",              // deploy | update | destroy
  "tenant_id": "a1c2...",
  "status": "accepted",
  "created_at": "2026-10-11T10:15:30.123Z"
}
```

Routing key: `task.<type>`. AMQP `message_id` = task id, so consumers can dedupe on it.

### Inbound message (worker progress update)

```json
{
  "update_id": "f3d9...",        // UUID, idempotency key, unique per update
  "task_id": "6b0f...",          // which task this updates
  "status": "in_progress",       // in_progress | done | failed
  "error": null,                 // string when status == failed
  "occurred_at": "2026-10-11T10:15:31.001Z"
}
```

Routing key: `task.update`.

### Outbox publisher loop

```
loop:
  BEGIN
    rows = SELECT * FROM outbox WHERE published_at IS NULL
           ORDER BY id LIMIT :batch FOR UPDATE SKIP LOCKED
    for row in rows:
        publish(row) with publisher confirm      # raises on nack / connection loss
        UPDATE outbox SET published_at = now() WHERE id = row.id
  COMMIT
  if no rows: wait on asyncio.Event (set by API after commit) or 500 ms poll
on error: rollback, exponential backoff with jitter, reconnect
```

Guarantees and honest limits:

- **Never before commit, never on rollback**: the outbox row only becomes visible when the business transaction commits.
- **At-least-once**: a crash after publish but before marking `published_at` re-sends. That is acceptable because every consumer dedupes by id.
- `SKIP LOCKED` lets multiple publisher instances run safely.

### Inbound consumer: `apply_update(update)` algorithm

One DB transaction per message:

```
1. parse + validate envelope               -> invalid? send to DLQ (permanent)
2. INSERT INTO processed_updates (update_id, task_id)
      ON CONFLICT DO NOTHING
   rowcount == 0                           -> duplicate: ack, done
3. SELECT task FOR UPDATE (by task_id)
   not found                               -> permanent: DLQ (or log + ack, see decision D3)
4. if task.status is terminal              -> stale: log, ack (processed row kept)
5. if new_status not in TASK_TRANSITIONS[task.status]
                                           -> stale/out-of-order: log, ack
6. UPDATE task SET status, error, updated_at
7. if new_status is terminal:
      UPDATE tenants
         SET status = :new, version = version + 1, updated_at = :now
       WHERE id = :tenant_id AND status = :expected
      0 rows -> log anomaly (tenant not in expected state), still commit task change
8. COMMIT, then ack message
```

The dedupe insert and the state change share one transaction, so a crash can never record "processed" without applying the change, or the reverse.

### Retry, backoff, poison messages

| Failure | Class | Handling |
|---|---|---|
| Malformed JSON / schema invalid | Permanent | `reject(requeue=False)` → DLQ, consumer continues |
| Unknown `task_id` | Permanent | DLQ with `x-error` header |
| Stale, duplicate, out-of-order | Not a failure | Log, ack |
| DB connection / deadlock / serialization error | Transient | In-process retry with exponential backoff + jitter (e.g. 0.2 s doubling to 10 s, max 5 attempts), message stays unacked |
| Transient errors exhausted | Escalate | `reject(requeue=False)` → DLQ; `replay_dlq.py` moves them back |
| Broker connection lost | Transient | `connect_robust` reconnects; own backoff on initial connect |

A top-level `try/except` around each message guarantees one bad message never kills the consumer loop. Use a low prefetch (e.g. 10).

---

## 9. Worker simulator

`python -m cp.worker [options]`

| Option / env | Default | Meaning |
|---|---|---|
| `--fail-rate` / `WORKER_FAIL_RATE` | `0.0` | Probability a task ends `failed` |
| `--min-delay-ms` / `WORKER_MIN_DELAY_MS` | `500` | Minimum simulated work time |
| `--max-delay-ms` / `WORKER_MAX_DELAY_MS` | `3000` | Maximum simulated work time |
| `--concurrency` / `WORKER_CONCURRENCY` | `5` | Parallel tasks (prefetch) |
| `--duplicate-rate` / `WORKER_DUPLICATE_RATE` | `0.0` | *Optional extra:* re-send updates with the same `update_id` |
| `--seed` / `WORKER_SEED` | none | Reproducible randomness |

Per task: publish `in_progress` → sleep random delay in range → publish `done` or `failed` (with `error`) → ack the task message. Each update gets a fresh `update_id` (UUID). If the worker receives the same task twice (redelivery), it simulates again, and the control plane's guards absorb the extras.

Options are read at process start, so scenarios are triggered by restarting with different args.

---

## 10. Docker-based development (DB and RabbitMQ run manually)

### One-time: network and dependencies

```bash
docker network create cpnet

docker run -d --name cp-postgres --network cpnet -p 5432:5432 \
  -e POSTGRES_USER=cp -e POSTGRES_PASSWORD=cp_local_dev -e POSTGRES_DB=cp \
  -v cp_pgdata:/var/lib/postgresql/data \
  postgres:16-alpine

docker run -d --name cp-rabbit --network cpnet -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=cp -e RABBITMQ_DEFAULT_PASS=cp_local_dev \
  rabbitmq:3.13-management-alpine
```

The passwords above are throwaway local values. Real config comes from env vars; only `.env.example` is committed.

### `.env.example`

```
DATABASE_URL=postgresql+asyncpg://cp:cp_local_dev@cp-postgres:5432/cp
TEST_DATABASE_URL=postgresql+asyncpg://cp:cp_local_dev@cp-postgres:5432/cp_test
AMQP_URL=amqp://cp:cp_local_dev@cp-rabbit:5672/
LOG_LEVEL=INFO
OUTBOX_POLL_INTERVAL_MS=500
OUTBOX_BATCH_SIZE=50
CONSUMER_PREFETCH=10
CONSUMER_MAX_ATTEMPTS=5
```

### Dockerfile (dev-friendly, includes dev tools so `make check` runs in-container)

```dockerfile
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project
COPY . .
RUN uv sync --frozen
ENV PATH="/app/.venv/bin:$PATH"
RUN useradd -m app && chown -R app /app
USER app
EXPOSE 8000
CMD ["uvicorn", "cp.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

For the later "single boot command" you may split into a slim runtime stage and a dev stage. Not needed now.

### Makefile (shape)

```make
IMAGE   ?= cp-app
NET     ?= cpnet
RUN     = docker run --rm --network $(NET) --env-file .env
RUN_SRC = $(RUN) -v $(PWD):/app

build:      ; docker build -t $(IMAGE) .
migrate:    ; $(RUN) $(IMAGE) alembic upgrade head
run:        ; docker run --rm --name cp-api --network $(NET) --env-file .env -p 8000:8000 -v $(PWD)/src:/app/src $(IMAGE) uvicorn cp.main:create_app --factory --host 0.0.0.0 --reload
worker:     ; $(RUN) --name cp-worker $(IMAGE) python -m cp.worker $(ARGS)
fmt:        ; $(RUN_SRC) $(IMAGE) ruff format .
lint:       ; $(RUN_SRC) $(IMAGE) sh -c "ruff check . && ruff format --check . && mypy src"
sec:        ; $(RUN_SRC) $(IMAGE) sh -c "bandit -q -r src && pip-audit"
test:       ; $(RUN_SRC) $(IMAGE) pytest --cov=cp --cov-report=term-missing
check:      lint sec test
```

Examples: `make worker ARGS="--fail-rate 1"`, `make worker ARGS="--min-delay-ms 5000 --max-delay-ms 10000"`.

### Test isolation (no Docker-in-Docker)

Integration tests connect to the already running Postgres and RabbitMQ:

- Session fixture creates a fresh `cp_test` database (drop and create, then run Alembic), and tests truncate tables between cases.
- RabbitMQ: each test run uses a unique name prefix (or vhost) for exchanges and queues, so tests never touch the dev queues.
- Later, the single boot command's compose file simply provides these same services to the test container.

---

## 11. Phased implementation plan

Rough effort assumes you are fluent with the stack. Commit after each numbered item.

### Phase 0: Scaffold and tooling (1-1.5 h)

- [ ] `pyproject.toml` (deps: fastapi, uvicorn, sqlalchemy[asyncio], asyncpg, alembic, aio-pika, pydantic-settings; dev: pytest, pytest-asyncio, pytest-cov, httpx, ruff, mypy, bandit, pip-audit)
- [ ] `Dockerfile`, `.dockerignore`, `.gitignore`, `.env.example`, `Makefile`
- [ ] `ruff` and `mypy` config, empty `src/cp` package, one smoke test, `make check` green
- [ ] Start the manual Postgres and RabbitMQ; confirm connectivity from the container

Commit: `chore: scaffold project, tooling and Dockerfile`

### Phase 1: Domain and persistence (2 h)

- [ ] `enums.py`, `state_machine.py` (tables from section 6), `domain/errors.py`
- [ ] SQLAlchemy models and Alembic migration `0001` (section 5)
- [ ] Session factory, repositories
- [ ] Unit tests: every valid and invalid tenant action, every task transition, terminal-outcome mapping

Commit: `feat(domain): state machines and persistence schema`

### Phase 2: Tenant and task REST API (3 h)

- [ ] Error classes → exception handlers → uniform error body (including 422 override)
- [ ] `POST /v1/tenants` (unique-violation handling, creates deploy task and outbox row in one txn)
- [ ] `GET` list and detail, cursor pagination, filters
- [ ] `PATCH` with atomic conditional update and error classification
- [ ] `DELETE` with atomic conditional update
- [ ] Task endpoints (`/v1/tasks`, `/v1/tasks/{id}`, `/v1/tenants/{id}/tasks`)
- [ ] ms-precision `Z` timestamp serializer
- [ ] `/healthz`, `/readyz`
- [ ] Integration tests for every route and error code

Commit(s): one per resource group.

### Phase 3: Outbox publisher and topology (2 h)

- [ ] `topology.py` declares exchanges and queues; `connection.py` robust connect with backoff
- [ ] Outbox publisher loop with publisher confirms, wake-up event, graceful shutdown
- [ ] Wire into FastAPI lifespan
- [ ] Tests: event appears after commit; nothing on rollback; crash-after-publish re-sends (simulate by skipping the `published_at` update)

Commit: `feat(messaging): transactional outbox and publisher`

### Phase 4: Worker simulator (1.5 h)

- [ ] CLI with options from section 9, env-var fallbacks, `--seed`
- [ ] Consume `cp.tasks.worker`, publish updates, ack after terminal update
- [ ] Manual smoke test: create a tenant and watch `accepted → in_progress → done` (updates still unconsumed at this point; verify via the RabbitMQ UI)

Commit: `feat(worker): provisioning simulator CLI`

### Phase 5: Inbound consumer (3 h). The most important phase

- [ ] `contracts.py` validation, `services/task_updates.py` per algorithm in section 8
- [ ] Consumer loop with retry/backoff classification and DLQ routing
- [ ] Wire into lifespan, graceful shutdown
- [ ] End-to-end: tenant goes `provisioning → active` with no manual steps

Commit: `feat(messaging): idempotent, order-tolerant update consumer`

### Phase 6: Test hardening and load script (3 h)

See section 12.

Commit: `test: concurrency, idempotency, ordering, poison message suites`

### Phase 7: Quality gates and CI (1 h)

- [ ] `make check` runs lint, format check, mypy, bandit, pip-audit, tests with coverage
- [ ] GitHub Actions workflow runs the same thing with service containers for Postgres and RabbitMQ

Commit: `ci: quality gates workflow`

### Phase 8: Docs (2 h)

See section 14.

Commit: `docs: README, architecture and runbook`

### Phase 9: Single-command boot (later, 1.5 h)

- [ ] `docker-compose.yml` with `postgres`, `rabbitmq`, `migrate` (one-shot), `api`, `worker`; healthchecks and `depends_on: condition: service_healthy`
- [ ] `make up` = `docker compose up --build -d`; `make test` runs the suite in a container against compose services
- [ ] Verify with a cold clone: `git clone && make up && make test`

Commit: `feat: one-command boot with docker compose`

Total: roughly 20 hours of focused work.

---

## 12. Test plan

### Unit (no I/O)

- Tenant action guards: all 6 statuses × `patch`/`delete`
- Task transition table: all 4 × 3 statuses
- Terminal outcome → tenant status mapping (6 cases)
- Slug validation: valid and invalid samples (length 2, 3, 28, 29, uppercase, leading digit, trailing hyphen, etc.)
- Cursor encode/decode, timestamp formatter (ms, `Z`, truncation)

### Integration, API

- Create → 201, `Location`, deploy task `accepted`, tenant `provisioning`, version 1
- Duplicate slug → 409 `tenant_already_exists` (also against a `destroyed` tenant)
- PATCH from `active` → 202; from other statuses → 409 `tenant_update_not_allowed`; wrong version → 409 `tenant_version_conflict`
- DELETE from `active` and `failed` → 202; from `provisioning`, `updating`, `destroying`, `destroyed` → 409
- 404s for unknown tenant and task, 422 for validation
- Pagination: stable ordering, no dupes or gaps across pages, `next_cursor` null at end
- Every error response matches the structured body

### Integration, concurrency (the proof)

- **N parallel PATCH with the same `version`** → exactly one 202, the rest 409; final tenant version is old+1; exactly one `update` task and one outbox row exist
- **N parallel POST with the same slug** → exactly one 201, the rest `tenant_already_exists`
- **Parallel PATCH + DELETE on an `active` tenant** → exactly one wins; one open task only
- Run each several times in a loop (flaky-race detection)

### Integration, messaging

- Outbox: event published after commit; none on rollback; at-least-once on simulated crash
- Published event contains `id`, `type`, `tenant_id`, `status`
- Consumer idempotency: same `update_id` twice → one state change, no error, second is a no-op
- Order tolerance: `done` then `in_progress` → ignored; any update to a terminal task → ignored; tenant not regressed
- Terminal outcome moves the tenant and bumps its version in the same transaction
- Poison messages: invalid JSON, wrong schema, unknown `task_id` → land in DLQ, consumer keeps processing the next valid message
- Transient failure: inject a DB error once → retried and eventually applied
- End-to-end with a live worker: tenant reaches `active`; with `fail_rate=1` reaches `failed`

### Load / race demo (`scripts/load_test.py`)

Against the running service: fire e.g. 50 concurrent PATCHes with one version, 50 concurrent creates with one slug, and print a summary table of status code counts plus a final consistency check (versions, task counts). Reference its output in the README.

---

## 13. Quality gates (`make check`)

| Gate | Tool |
|---|---|
| Lint | `ruff check` |
| Format | `ruff format --check` (and `make fmt` to fix) |
| Types | `mypy --strict` on `src` (relax per-module only if needed) |
| Security | `bandit -r src`, `pip-audit` |
| Tests | `pytest --cov=cp --cov-fail-under=85` |

Also: no secrets in the repo (`.env` ignored, only `.env.example`). Optionally add `gitleaks` or a pre-commit hook.

---

## 14. README and docs outline

1. Overview and what it does
2. Architecture diagram (Mermaid from section 3) and component responsibilities
3. Quick start: exact commands (clone, boot, test); a "run dependencies manually" note during dev
4. API usage with `curl` for every endpoint, a full lifecycle walkthrough, link to `openapi.yaml` and `/docs`
5. Message schemas and topology (both directions)
6. Design decisions and trade-offs: outbox vs direct publish, at-least-once + idempotency, optimistic locking via conditional UPDATE, error precedence, strict transition table, why RabbitMQ, why not Celery
7. Failure scenario runbook (below)
8. Quality gates and how to run each
9. Known limitations and what you would do next

### Failure scenario runbook (copy into the README)

Worker always fails:

```bash
make worker ARGS="--fail-rate 1"
curl -s -X POST localhost:8000/v1/tenants -H 'content-type: application/json' \
  -d '{"slug":"acme","name":"Acme"}'
curl -s localhost:8000/v1/tenants    # -> status "failed"
```

Slow worker:

```bash
make worker ARGS="--min-delay-ms 10000 --max-delay-ms 15000"
```

Inject a duplicate or out-of-order update through the RabbitMQ HTTP API (swap the ids):

```bash
curl -u cp:cp_local_dev -H 'content-type: application/json' -X POST \
  http://localhost:15672/api/exchanges/%2F/cp.task-updates/publish -d '{
    "properties": {"delivery_mode": 2, "content_type": "application/json"},
    "routing_key": "task.update",
    "payload": "{\"update_id\":\"11111111-1111-4111-8111-111111111111\",\"task_id\":\"<TASK_ID>\",\"status\":\"in_progress\",\"occurred_at\":\"2026-10-11T10:00:00.000Z\"}",
    "payload_encoding": "string"
  }'
```

Send it twice (duplicate), or send `in_progress` after the task is `done` (stale). Poison message: use `"payload": "not json"`. Inspect with the management UI at `http://localhost:15672` (DLQ: `cp.task-updates.dlq`). `scripts/publish_update.py` wraps the same thing.

---

## 15. Decision points to settle early

| # | Question | Recommendation |
|---|---|---|
| D1 | A `done`/`failed` update arrives for a task still `accepted` (the `in_progress` was lost or reordered). Apply or ignore? | **Strict**: ignore, per the diagram (`accepted → in_progress → terminal`). Document the trade-off: a lost `in_progress` can strand a task, mitigated by worker redelivery re-simulating the full sequence. A lenient mode (apply implied intermediate) is a short follow-up if you prefer. |
| D2 | PATCH check order | not found → version conflict → status guard |
| D3 | Update for an unknown `task_id` | DLQ (it is not recoverable by retrying), log with context |
| D4 | `processed_updates` growth | Document it; note a periodic prune (e.g. older than 7 days) as a next step |
| D5 | Publisher and consumer in the API process vs separate | In-process now; structured so they can be split |
| D6 | Stuck tasks if the worker dies mid-task | Known limitation; stretch goal: a sweeper that fails tasks open longer than a timeout |

---

## 16. Likely walkthrough questions

- *Why an outbox and not publish-after-commit in the request handler?* The process can die between commit and publish, losing the event. The outbox makes the event part of the same atomic write.
- *What if the publisher crashes after publishing but before marking it sent?* A duplicate event. Consumers are idempotent, so this is safe.
- *How does PATCH avoid lost updates?* A single conditional `UPDATE ... WHERE version = :v AND status = 'active'`; the database serializes writers on the row.
- *Why is the dedupe insert in the same transaction as the state change?* Otherwise a crash between them either skips a real update or applies it twice.
- *What does `FOR UPDATE SKIP LOCKED` buy you?* Multiple publishers can run without blocking or double-publishing the same row.
- *How are poison messages handled?* Permanent failures go straight to the DLQ; transient ones retry with backoff, then go to the DLQ; the consumer loop never dies.
- *Why not Celery / Kafka / exactly-once?* Explicit contracts and delivery semantics are the point of the exercise; exactly-once does not exist, so the design is at-least-once plus idempotency.