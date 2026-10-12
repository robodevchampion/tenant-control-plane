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

Run the end-to-end scenario script from PowerShell on Windows, or from a shell with a working Podman Compose provider:

```bash
uv run python scripts/podman_e2e_flow.py 1
```

The script requires Python development dependencies (`uv sync --all-groups`) and a running Podman Compose stack. It creates a uniquely named tenant and polls the tenant/task endpoints. Use `uv run python scripts/podman_e2e_flow.py --help` to list the scenarios. Running from WSL requires a Compose provider configured for that WSL Podman environment; on Windows, PowerShell uses the Windows provider.

### Scenario 1: Successful deployment

Uses the default Compose worker, which has `WORKER_FAIL_RATE=0.0` and a 5-15 second work delay. Expected result: tenant `active`, task `done`.

### Scenario 2: Forced failure

```bash
uv run python scripts/podman_e2e_flow.py 2
```

The script stops the default worker, launches a temporary worker with `--fail-rate 1`, and expects both tenant and task to reach `failed`. It removes the temporary worker and restarts the default worker when it exits.

### Scenario 3: Duplicate updates

```bash
uv run python scripts/podman_e2e_flow.py 3
```

The script temporarily runs the worker with `--duplicate-rate 1`, causing every progress and terminal update to be published twice. It expects successful completion, two unique processed update records, and duplicate acknowledgements in the API logs. This demonstrates consumer idempotency.

### Scenario 4: Out-of-order updates

```bash
uv run python scripts/podman_e2e_flow.py 4
```

The script stops the worker, creates a tenant, then publishes a `done` update before an `in_progress` update using RabbitMQ's management HTTP API. It verifies the task remains `done` and the tenant remains `active` after the late progress message. It requires the management endpoint at `http://127.0.0.1:15673` and the local Compose credentials (`admin` / `change-me` by default). The worker is restarted automatically when the script exits.

### Scenario 5: Duplicate task event

```bash
uv run python scripts/podman_e2e_flow.py 5
```

After deployment completes, the script republishes the original accepted `task.deploy` event with the same task ID and payload through RabbitMQ's management API. The worker finds the cached terminal result and replays its `done` update; the consumer identifies that update by its stable ID and logs it as a duplicate. The script verifies the processed-update count remains two and the tenant/task remain `active`/`done`. This differs from scenario 3, which duplicates worker-update messages rather than the original task event.

Scenarios 4 and 5 require the management endpoint at `http://127.0.0.1:15673` and the local Compose credentials (`admin` / `change-me` by default). The script restarts the default worker automatically after scenarios that stop or replace it. All scenarios accept the API address through `API_BASE`; scenarios 4 and 5 accept the management address through `BROKER_MANAGEMENT_BASE`. Set `BROKER_USER` and `BROKER_PASSWORD` if the local broker credentials differ. `--fail-rate` accepts a decimal or ratio (for example `0.2` or `1/5`); it is the approximate fraction of tasks that ultimately fail.

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
uv run python scripts/check.py
```

The single check command runs Ruff lint, Ruff format validation, mypy over `src`, Bandit over `src`, pip-audit against the locked runtime and development dependencies, and pytest. The dependency audit requires network access to fetch vulnerability data. Integration tests need PostgreSQL and RabbitMQ; tests that cannot reach those services may skip with a reason.