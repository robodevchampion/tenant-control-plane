# tenant-control-plane
Multi-tenant provisioning control plane in Python, built on the transactional outbox pattern with RabbitMQ.

## Local Podman stack

This repo includes a Podman-compatible compose file for a local three-service stack:

- `api`: FastAPI app with the transactional outbox publisher and update consumer
- `worker`: worker simulator
- `postgres`: PostgreSQL on port `5433`
- `rabbitmq`: RabbitMQ on ports `5673` and `15673`

### Start the stack

```bash
podman compose up --build -d
```

### Stop the stack

```bash
podman compose down -v
```

### Port mapping

The host ports are intentionally different from the ports the application sees inside the compose network:

- Host access: `postgres` at `127.0.0.1:5433`, `rabbitmq` at `127.0.0.1:5673`
- Container access: `postgres:5432`, `rabbitmq:5672`

This is the correct Docker/Podman pattern for compose networking.

### Verify the app is alive

```bash
curl http://127.0.0.1:8000/healthz
```

### Run the end-to-end flow

```bash
./scripts/podman_e2e_flow.sh
```

This creates a tenant through the API and waits for the worker to complete the deploy task and move the tenant to `active`.

### Example negative scenarios

```bash
podman compose up -d --build
podman compose run --rm worker python -m function.worker --fail-rate 1 --min-delay-ms 5000 --max-delay-ms 10000
```

This forces all tasks to fail, which is useful for verifying failure handling.
