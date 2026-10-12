# Architecture and State Flow

## Components

- **HTTP API** validates tenant operations and reads tenant/task state.
- **PostgreSQL** stores tenants, tasks, outbox events, and processed worker updates.
- **Outbox publisher** publishes committed task events to RabbitMQ and marks them published.
- **Worker simulator** consumes accepted tasks and emits `in_progress`, then `done` or `failed` updates.
- **Update consumer** applies task updates and terminal tenant status changes.
- **RabbitMQ** carries task events to workers and worker updates back to the API service.

The API container runs the outbox publisher and update consumer in its application lifespan. The worker is a separate process/container.

## Tenant and Task Lifecycle

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as Control Plane API
    participant DB as PostgreSQL
    participant Outbox as Outbox Publisher
    participant Broker as RabbitMQ
    participant Worker as Worker Simulator
    participant Consumer as Update Consumer

    Client->>API: POST /v1/tenants
    API->>DB: Transaction: tenant(provisioning) + task(accepted) + outbox
    DB-->>API: Commit
    API-->>Client: 201 tenant + task
    Outbox->>DB: Read unpublished outbox event
    Outbox->>Broker: Publish task event
    Outbox->>DB: Mark event published
    Broker->>Worker: Deliver task
    Worker->>Broker: Publish in_progress update
    Broker->>Consumer: Deliver update
    Consumer->>DB: Task becomes in_progress
    Worker->>Broker: Publish done or failed update
    Broker->>Consumer: Deliver terminal update
    Consumer->>DB: Update task and tenant status
    Client->>API: GET tenant and task
    API-->>Client: Current statuses and versions
```

The lifecycle states are:

```mermaid
stateDiagram-v2
    state "Tenant" as Tenant {
        [*] --> provisioning
        provisioning --> active: deploy done
        provisioning --> failed: deploy failed
        active --> updating: PATCH accepted
        updating --> active: update done
        updating --> failed: update failed
        active --> destroying: DELETE accepted
        failed --> destroying: DELETE accepted
        destroying --> destroyed: destroy done
        destroying --> failed: destroy failed
    }
    state "Task" as Task {
        [*] --> accepted
        accepted --> in_progress: worker update
        in_progress --> done: worker result
        in_progress --> failed: worker result
    }
```

## Design Decisions

### Transactional outbox

The tenant mutation, task, and outbox event are stored in one database transaction. The publisher sends the event only after it can read the committed outbox row. If publishing fails, the row remains available for a later attempt; this avoids losing a task between a database commit and broker publication.

### At-least-once delivery and idempotency

RabbitMQ delivery and publishing can be retried. Each worker update has a stable `update_id`, recorded in PostgreSQL before its state change is applied. Repeated updates are ignored. Task transitions also reject stale changes so a late `in_progress` update cannot regress a terminal task.

### Tenant version semantics

Tenant `version` is used for optimistic locking of user-requested mutations. Creating a tenant starts at version 1; an accepted PATCH increments it once. Worker completion changes the tenant status and timestamp but does not increment the version. For example, a tenant stays at v1 after initial deployment, and a later accepted update advances it from v1 to v2, not v3 when the worker finishes.

### Local networking

Compose maps host ports to container ports: PostgreSQL `5433:5432`, RabbitMQ AMQP `5673:5672`, and RabbitMQ management `15673:15672`. Containers use service DNS names and internal ports; host port mappings are for tools running outside Compose.