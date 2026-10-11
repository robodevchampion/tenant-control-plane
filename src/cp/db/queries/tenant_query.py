from datetime import datetime
from uuid import UUID

from sqlalchemy import select

from cp.db.models import Tenant
from cp.db.queries.base_query import BaseQuery
from cp.domain.enums import TenantStatus


class TenantQuery(BaseQuery):
    async def get_by_id(self, tenant_id: UUID) -> Tenant | None:
        return await self.session.get(Tenant, tenant_id)

    async def get_by_slug(self, slug: str) -> Tenant | None:
        return await self.session.scalar(select(Tenant).where(Tenant.slug == slug))

    async def list(
        self,
        *,
        status: TenantStatus | None,
        slug: str | None,
        cursor: tuple[datetime, UUID] | None,
        limit: int,
    ) -> tuple[list[Tenant], bool]:
        statement = select(Tenant)
        if status is not None:
            statement = statement.where(Tenant.status == status)
        if slug is not None:
            statement = statement.where(Tenant.slug == slug)
        if cursor is not None:
            created_at, item_id = cursor
            statement = statement.where(
                (Tenant.created_at < created_at)
                | ((Tenant.created_at == created_at) & (Tenant.id < item_id))
            )
        statement = statement.order_by(Tenant.created_at.desc(), Tenant.id.desc()).limit(limit + 1)
        rows = list((await self.session.scalars(statement)).all())
        return rows[:limit], len(rows) > limit
