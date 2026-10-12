# Local Setup

This guide boots the API, worker, PostgreSQL, and RabbitMQ with Podman Compose.

## Prerequisites

- Git
- Podman (on Windows, install Podman Desktop or the Podman machine and start its machine)
- A Compose provider supported by `podman compose` (check with `podman compose version`)
- `curl` for the health check and smoke test
- Bash to run `scripts/podman_e2e_flow.sh`; on Windows, Git Bash or WSL works

Check that Podman can reach its engine before building:

```bash
podman info
podman compose version
```

On Windows, start the Podman machine from Podman Desktop or run `podman machine start` in PowerShell if it is stopped.

## Clone and Start

Run these commands from PowerShell, Git Bash, or a POSIX shell:

```bash
git clone https://github.com/robodevchampion/tenant-control-plane.git
cd tenant-control-plane
podman compose up --build -d
```

The first build downloads the base image and installs system and Python dependencies, so it can take a few minutes. The API container applies Alembic migrations before starting the HTTP server.

Check service status and startup logs:

```bash
podman compose ps
podman compose logs -f api worker
```

Stop following logs with Ctrl+C; this does not stop the containers.

## Local Endpoints

| Service | Host address |
| --- | --- |
| API | `http://localhost:8000` |
| Swagger UI | `http://localhost:8000/docs` |
| API readiness | `http://localhost:8000/readyz` |
| RabbitMQ management | `http://localhost:15673` |
| PostgreSQL | `localhost:5433` |
| RabbitMQ AMQP | `localhost:5673` |

Ports inside the Compose network are different from the host ports: the API and worker connect to `postgres:5432` and `rabbitmq:5672`. The host mappings are `5433:5432` and `5673:5672`.

The local compose file currently uses `admin` / `change-me` for the database and broker. These are development credentials, not production secrets.

## Verify the Full Flow

Check health and readiness:

```bash
curl -i http://localhost:8000/healthz
curl -i http://localhost:8000/readyz
```

Run the end-to-end smoke script from the repository root:

```bash
bash ./scripts/podman_e2e_flow.sh
```

The script creates a tenant, then polls the tenant and task endpoints until the tenant is `active` and its deploy task is `done`. It requires Bash, Python, and `curl` on the host. The API response includes the created tenant and task IDs; those can also be checked in Swagger UI.

The Compose worker is configured with `--fail-rate 0.2` to exercise failures during normal local use, so a smoke-test tenant can fail by design. For a deterministic success run, temporarily set the worker's `--fail-rate` to `0` in `compose.yaml`, recreate the worker with `podman compose up -d --force-recreate worker`, then run the script.

## Reproduce Worker Failures

To route tasks to a worker configured to fail every task, stop the default worker and start a temporary worker with a 100% failure rate:

```bash
podman compose stop worker
podman compose run --rm worker python -m function.worker --fail-rate 1 --min-delay-ms 500 --max-delay-ms 1000
```

Create a tenant while this worker is running. The deploy task should reach `failed`, and the tenant should reach `failed`. Stop the temporary worker with Ctrl+C, then restore the default worker:

```bash
podman compose up -d worker
```

`--fail-rate` accepts a decimal or ratio, such as `0.2` or `1/5`; the value is the approximate fraction of tasks that ultimately fail, not the fraction of individual attempts.

## Stop and Remove Data

Stop and remove the containers while retaining the PostgreSQL volume:

```bash
podman compose down
```

To also delete the local database volume and all persisted tenant/task data:

```bash
podman compose down -v
```

## Troubleshooting

- If `podman compose` cannot find a provider, install/configure a Compose provider supported by Podman, then check `podman compose version` again.
- If Podman reports that its internal status is invalid or the engine is unavailable, check `podman machine list`, start the machine, and verify `podman info` before retrying the build.
- If a host port is already occupied, stop the process using it or update the host side of the port mapping in `compose.yaml`.
- If a service is unhealthy, inspect it with `podman compose logs postgres rabbitmq api worker`.

## Run Tests and Quality Checks

Install Python 3.12 and `uv`, then run from the repository root:

```bash
uv sync --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

The broker/database integration tests use the configured local services; tests that require unavailable dependencies may skip with a reason.