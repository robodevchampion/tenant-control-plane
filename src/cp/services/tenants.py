from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from cp.db.models import Outbox, Task, Tenant
from cp.db.queries import TenantQuery
from cp.db.repositories import OutboxRepository, TaskRepository, TenantRepository
from cp.domain.enums import TaskStatus, TaskType, TenantStatus
from cp.domain.errors import DomainError
from cp.domain.state_machine import TENANT_ACTION_GUARDS
from cp.serialization import format_timestamp


def _constraint_name(error: BaseException) -> str | None:
    pending = [error]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))

        name = getattr(current, "constraint_name", None) or getattr(
            getattr(current, "diag", None), "constraint_name", None
        )
        if name is not None:
            return str(name)

        for attribute in ("__cause__", "__context__", "orig"):
            nested = getattr(current, attribute, None)
            if isinstance(nested, BaseException):
                pending.append(nested)
    return None


def _task_event(task: Task) -> dict[str, str]:
    return {
        "id": str(task.id),
        "type": task.type.value,
        "tenant_id": str(task.tenant_id),
        "status": task.status.value,
        "created_at": format_timestamp(task.created_at),
    }


async def _raise_open_task_conflict(session: AsyncSession, tenant_id: UUID, action: str) -> None:
    current = await TenantQuery(session).get_by_id(tenant_id)
    raise DomainError(
        "tenant_update_not_allowed",
        f"Action '{action}' is not allowed while tenant {tenant_id} already has an open task.",
        409,
        {
            "tenant_id": str(tenant_id),
            "action": action,
            "current_status": current.status.value if current is not None else None,
        },
    )


async def _ensure_no_open_task(session: AsyncSession, tenant_id: UUID, action: str) -> None:
    result = await session.execute(
        select(Task.id)
        .where(
            Task.tenant_id == tenant_id,
            Task.status.in_((TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS)),
        )
        .limit(1)
    )
    if result.scalar_one_or_none() is not None:
        await _raise_open_task_conflict(session, tenant_id, action)


async def _add_task(
    session: AsyncSession, tenant: Tenant, task_type: TaskType, now: datetime
) -> Task:
    await _ensure_no_open_task(session, tenant.id, task_type.value)
    task = Task(
        id=uuid4(),
        type=task_type,
        tenant_id=tenant.id,
        status=TaskStatus.ACCEPTED,
        created_at=now,
        updated_at=now,
    )
    try:
        await TaskRepository(session).add_task(task)
    except IntegrityError as exc:
        if _constraint_name(exc) == "tasks_one_open_per_tenant":
            await _raise_open_task_conflict(session, tenant.id, task_type.value)
        raise
    await OutboxRepository(session).add_event(
        Outbox(
            task_id=task.id,
            routing_key=f"task.{task_type.value}",
            payload=_task_event(task),
            created_at=now,
        )
    )
    return task


async def create_tenant(session: AsyncSession, slug: str, name: str) -> tuple[Tenant, Task]:
    now = datetime.now(UTC)
    tenant = Tenant(
        id=uuid4(),
        slug=slug,
        name=name,
        status=TenantStatus.PROVISIONING,
        version=1,
        created_at=now,
        updated_at=now,
    )
    try:
        async with session.begin():
            await TenantRepository(session).add(tenant)
            task = await _add_task(session, tenant, TaskType.DEPLOY, now)
    except IntegrityError as exc:
        if _constraint_name(exc) == "tenants_slug_key":
            raise DomainError(
                "tenant_already_exists",
                f"A tenant with slug '{slug}' already exists.",
                409,
                {"slug": slug},
            ) from exc
        raise
    return tenant, task


async def update_tenant(
    session: AsyncSession, tenant_id: UUID, version: int, name: str
) -> tuple[Tenant, Task]:
    allowed_statuses, next_status, task_type = TENANT_ACTION_GUARDS["patch"]
    now = datetime.now(UTC)
    async with session.begin():
        tenant = await TenantRepository(session).transition(
            tenant_id,
            allowed_statuses,
            next_status,
            now,
            version=version,
            name=name,
        )
        if tenant is None:
            current = await TenantQuery(session).get_by_id(tenant_id)
            if current is None:
                raise _tenant_not_found(tenant_id)
            if current.version != version:
                raise DomainError(
                    "tenant_version_conflict",
                    f"Tenant version is {current.version} but the request used {version}.",
                    409,
                    {"provided_version": version, "current_version": current.version},
                )
            raise _action_not_allowed("patch", current.status, allowed_statuses)
        task = await _add_task(session, tenant, task_type, now)
    return tenant, task


async def delete_tenant(session: AsyncSession, tenant_id: UUID) -> tuple[Tenant, Task]:
    now = datetime.now(UTC)
    allowed_statuses, next_status, task_type = TENANT_ACTION_GUARDS["delete"]
    async with session.begin():
        tenant = await TenantRepository(session).transition(
            tenant_id,
            allowed_statuses,
            next_status,
            now,
        )
        if tenant is None:
            current = await TenantQuery(session).get_by_id(tenant_id)
            if current is None:
                raise _tenant_not_found(tenant_id)
            raise _action_not_allowed("delete", current.status, allowed_statuses)
        task = await _add_task(session, tenant, task_type, now)
    return tenant, task


def _tenant_not_found(tenant_id: UUID) -> DomainError:
    return DomainError(
        "tenant_not_found",
        f"Tenant {tenant_id} was not found.",
        404,
        {"tenant_id": str(tenant_id)},
    )


def _action_not_allowed(
    action: str, current_status: TenantStatus, allowed: frozenset[TenantStatus]
) -> DomainError:
    return DomainError(
        "tenant_update_not_allowed",
        f"Action '{action}' is not allowed while the tenant is '{current_status.value}'.",
        409,
        {
            "action": action,
            "current_status": current_status.value,
            "allowed_statuses": sorted(value.value for value in allowed),
        },
    )
