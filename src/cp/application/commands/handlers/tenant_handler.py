from sqlalchemy.ext.asyncio import AsyncSession

from cp.application.commands.tenant_commands import (
    CreateTenantCommand,
    DeleteTenantCommand,
    GetTenantBySlugCommand,
    GetTenantCommand,
    ListTenantsCommand,
    UpdateTenantCommand,
)
from cp.application.core.mediator import Mediator
from cp.application.core.pagination import decode_cursor, encode_cursor
from cp.application.dto import TenantDTO, TenantListDTO, TenantWithTaskDTO
from cp.db.queries import TenantQuery
from cp.domain.errors import DomainError
from cp.services.tenants import create_tenant, delete_tenant, update_tenant


@Mediator.register_handler(CreateTenantCommand)
class CreateTenantHandler:
    async def handle(
        self, command: CreateTenantCommand, session: AsyncSession
    ) -> TenantWithTaskDTO:
        tenant, task = await create_tenant(session, command.slug, command.name)
        return TenantWithTaskDTO(tenant=tenant, task=task)


@Mediator.register_handler(ListTenantsCommand)
class ListTenantsHandler:
    async def handle(self, command: ListTenantsCommand, session: AsyncSession) -> TenantListDTO:
        cursor = decode_cursor(command.cursor) if command.cursor else None
        rows, has_more = await TenantQuery(session).list(
            status=command.status,
            slug=command.slug,
            cursor=cursor,
            limit=command.limit,
        )
        next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id) if has_more else None
        return TenantListDTO(items=rows, next_cursor=next_cursor)


@Mediator.register_handler(GetTenantBySlugCommand)
class GetTenantBySlugHandler:
    async def handle(self, command: GetTenantBySlugCommand, session: AsyncSession) -> TenantDTO:
        tenant = await TenantQuery(session).get_by_slug(command.slug)
        if tenant is None:
            raise DomainError(
                "tenant_not_found",
                f"Tenant with slug '{command.slug}' was not found.",
                404,
                {"slug": command.slug},
            )
        return TenantDTO.model_validate(tenant)


@Mediator.register_handler(GetTenantCommand)
class GetTenantHandler:
    async def handle(self, command: GetTenantCommand, session: AsyncSession) -> TenantDTO:
        tenant = await TenantQuery(session).get_by_id(command.tenant_id)
        if tenant is None:
            raise DomainError(
                "tenant_not_found",
                f"Tenant {command.tenant_id} was not found.",
                404,
                {"tenant_id": str(command.tenant_id)},
            )
        return TenantDTO.model_validate(tenant)


@Mediator.register_handler(UpdateTenantCommand)
class UpdateTenantHandler:
    async def handle(
        self, command: UpdateTenantCommand, session: AsyncSession
    ) -> TenantWithTaskDTO:
        tenant, task = await update_tenant(
            session, command.tenant_id, command.version, command.name
        )
        return TenantWithTaskDTO(tenant=tenant, task=task)


@Mediator.register_handler(DeleteTenantCommand)
class DeleteTenantHandler:
    async def handle(
        self, command: DeleteTenantCommand, session: AsyncSession
    ) -> TenantWithTaskDTO:
        tenant, task = await delete_tenant(session, command.tenant_id)
        return TenantWithTaskDTO(tenant=tenant, task=task)
