# Tenant Control Plane

A Python control-plane API for asynchronous tenant provisioning. The API stores each requested operation as a task, publishes it through a transactional outbox, and applies worker updates from RabbitMQ. PostgreSQL provides durable state; Podman Compose runs the complete local stack.

## Quick Start

See [Local Setup](docs/setup.md) for prerequisites, first boot, verification, shutdown, and troubleshooting. In brief:

```bash
git clone https://github.com/robodevchampion/tenant-control-plane.git
cd tenant-control-plane
podman compose up --build -d
```

The API is available at `http://localhost:8000`.

## Run the Test and Quality Gate

Install the development tools and run the full documented gate from the repository root:

```bash
uv sync --all-groups
uv run python scripts/check.py
```

The single check command runs linting, formatting validation, type checking, source security scanning, dependency auditing, and the test suite. The dependency audit needs network access to query vulnerability advisories.

## API Documentation

- Interactive Swagger UI for a running local stack: [http://localhost:8000/docs](http://localhost:8000/docs)
- OpenAPI specification in this repository: [openapi.yaml](openapi.yaml)
- OpenAPI in Swagger Editor: [View the specification](https://editor.swagger.io/?url=https://raw.githubusercontent.com/robodevchampion/tenant-control-plane/dev/openapi.yaml)
- Request examples, routes, and error responses: [API Guide](docs/api.md)

## How It Works

The API and worker communicate through RabbitMQ. Tenant and task writes are committed together; the outbox publisher then sends the task, and the worker publishes progress and a terminal result. See the [Architecture and State Flow](docs/architecture.md) for the diagram and design notes.

## Documentation Map

| Document | What it covers |
| --- | --- |
| [Local Setup](docs/setup.md) | Podman prerequisites, clone and boot commands, ports, smoke test, worker scenarios, shutdown, troubleshooting, and tests |
| [Architecture and State Flow](docs/architecture.md) | Components, Mermaid lifecycle diagram, persistence and messaging decisions, idempotency, and tenant version semantics |
| [API Guide](docs/api.md) | Swagger links, tenant/task routes, curl examples, pagination, optimistic locking, and structured errors |
| [OpenAPI specification](openapi.yaml) | Machine-readable API contract |

## Stack

- Python 3.12, FastAPI, and SQLAlchemy async
- PostgreSQL with Alembic migrations
- RabbitMQ with a transactional outbox and idempotent update consumer
- A separately run worker simulator
- Podman Compose for local services

## License and Secrets

Do not commit real credentials. The compose defaults are for local development only; replace them before using this stack in a shared or production environment.
