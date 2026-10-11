from datetime import datetime
from uuid import UUID

from sqlalchemy import update

from cp.db.models import Tenant
from cp.db.repositories.base_repository import BaseRepository
from cp.domain.enums import TenantStatus


class TenantRepository(BaseRepository):
    async def transition(
        self,
        tenant_id: UUID,
        allowed_statuses: frozenset[TenantStatus],
        next_status: TenantStatus,
        now: datetime,
        version: int | None = None,
        name: str | None = None,
    ) -> Tenant | None:
        conditions = [Tenant.id == tenant_id, Tenant.status.in_(allowed_statuses)]
        if version is not None:
            conditions.append(Tenant.version == version)
        values: dict[str, object] = {
            "status": next_status,
            "version": Tenant.version + 1,
            "updated_at": now,
        }
        if name is not None:
            values["name"] = name
        result = await self.session.execute(
            update(Tenant).where(*conditions).values(**values).returning(Tenant)
        )
        return result.scalar_one_or_none()
