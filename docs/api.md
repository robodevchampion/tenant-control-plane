# API Guide

The API is served at `http://localhost:8000` by default.

## Interactive and Machine-Readable Docs

- [Swagger UI](http://localhost:8000/docs) is available when the local API is running.
- [OpenAPI source](../openapi.yaml) is checked into this repository.
- [OpenAPI in Swagger Editor](https://editor.swagger.io/?url=https://raw.githubusercontent.com/robodevchampion/tenant-control-plane/dev/openapi.yaml) loads the public specification without starting the local stack.

## Tenant Operations

Create a tenant. The response contains the tenant and the accepted deploy task:

```bash
curl -i -X POST http://localhost:8000/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"acme-prod","name":"Acme Production"}'
```

List tenants, optionally filtering by status, slug, or pagination cursor:

```bash
curl 'http://localhost:8000/v1/tenants?status=active&limit=20'
```

Get a tenant by ID or slug:

```bash
curl http://localhost:8000/v1/tenants/{tenant_id}
curl http://localhost:8000/v1/tenants/by-slug/acme-prod
```

Partially update a tenant with the version last read from the API. An accepted update responds with the `updating` tenant and its task; provisioning continues asynchronously:

```bash
curl -i -X PATCH http://localhost:8000/v1/tenants/{tenant_id} \
  -H 'Content-Type: application/json' \
  -d '{"version":1,"name":"Acme Production EU"}'
```

Delete a tenant. Deletion is asynchronous and returns the accepted destroy task:

```bash
curl -i -X DELETE http://localhost:8000/v1/tenants/{tenant_id}
```

## Task Tracking

Tasks are read-only over HTTP. List tasks, optionally filtering by tenant, status, or type; get one task by ID; or list a tenant's tasks:

```bash
curl 'http://localhost:8000/v1/tasks?tenant_id={tenant_id}'
curl http://localhost:8000/v1/tasks/{task_id}
curl http://localhost:8000/v1/tenants/{tenant_id}/tasks
```

Poll the tenant and task resources to observe asynchronous progress. Health endpoints are `GET /healthz` (process liveness) and `GET /readyz` (database and broker readiness).

## Version and Errors

The tenant version is an optimistic-lock token for user mutations. A successful PATCH advances it once when the request is accepted; worker progress and terminal status changes do not advance it. Send the latest version with every PATCH. A stale version returns HTTP 409 with code `tenant_version_conflict`.

Errors use a structured body such as:

```json
{
  "error": {
    "code": "tenant_update_not_allowed",
    "message": "The requested action is not allowed in the current tenant state.",
    "details": {}
  }
}
```

| Code | HTTP | Meaning |
| --- | --- | --- |
| `tenant_not_found` | 404 | Tenant ID or slug does not exist |
| `task_not_found` | 404 | Task ID does not exist |
| `tenant_already_exists` | 409 | Slug is already in use, including for a destroyed tenant |
| `tenant_version_conflict` | 409 | The supplied version is not current |
| `tenant_update_not_allowed` | 409 | The action is not allowed in the tenant's current state |
| `validation_error` | 422 | Request data or query parameter is invalid |