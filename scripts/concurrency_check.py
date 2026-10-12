import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import delete, select, update

from cp.db.models import Outbox, Task, Tenant
from cp.db.session import SessionFactory
from cp.domain.enums import TaskStatus, TenantStatus
from cp.domain.errors import DomainError
from cp.services.tenants import create_tenant, update_tenant


async def prepare_tenant() -> str:
    slug = f"race-demo-{uuid4().hex[:8]}"
    async with SessionFactory() as session:
        tenant, _ = await create_tenant(session, slug, "Race demo tenant")
        await session.execute(
            update(Tenant)
            .where(Tenant.id == tenant.id)
            .values(
                status=TenantStatus.ACTIVE,
                version=2,
                updated_at=datetime.now(UTC),
            )
        )
        await session.execute(
            update(Task)
            .where(Task.tenant_id == tenant.id)
            .values(status=TaskStatus.DONE, updated_at=datetime.now(UTC))
        )
        await session.commit()
        return str(tenant.id)


async def patch_tenant(tenant_id: str, name: str) -> dict[str, str | int | None]:
    async with SessionFactory() as session:
        try:
            tenant, task = await update_tenant(session, UUID(tenant_id), 2, name)
            return {
                "attempt": name,
                "status": "success",
                "version": tenant.version,
                "task_type": task.type.value,
            }
        except DomainError as exc:
            return {
                "attempt": name,
                "status": "conflict",
                "code": exc.code,
                "message": exc.message,
            }


async def cleanup(tenant_id: str) -> None:
    async with SessionFactory() as session:
        async with session.begin():
            task_ids = select(Task.id).where(Task.tenant_id == tenant_id)
            await session.execute(delete(Outbox).where(Outbox.task_id.in_(task_ids)))
            await session.execute(delete(Task).where(Task.tenant_id == tenant_id))
            await session.execute(delete(Tenant).where(Tenant.id == tenant_id))


async def main() -> None:
    tenant_id = await prepare_tenant()
    try:
        results = await asyncio.gather(
            patch_tenant(tenant_id, "first"),
            patch_tenant(tenant_id, "second"),
        )
        success_count = sum(1 for result in results if result["status"] == "success")
        conflict_count = sum(1 for result in results if result["status"] == "conflict")
        print(f"success={success_count} conflict={conflict_count}")
        print(results)
    finally:
        await cleanup(tenant_id)


if __name__ == "__main__":
    asyncio.run(main())
