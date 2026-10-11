from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from cp.application.commands import UpdateTenantBody
from cp.application.commands.task_commands import ListTenantTasksCommand
from cp.application.commands.tenant_commands import (
    CreateTenantCommand,
    DeleteTenantCommand,
    GetTenantBySlugCommand,
    GetTenantCommand,
    ListTenantsCommand,
    UpdateTenantCommand,
)
from cp.application.core.mediator import Mediator
from cp.application.dto import TaskListDTO, TenantDTO, TenantListDTO, TenantWithTaskDTO
from cp.db.session import get_session
from cp.domain.enums import TenantStatus

router = APIRouter(prefix="/v1/tenants", tags=["Tenants"])
SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("", response_model=TenantListDTO)
async def list_tenants(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
    tenant_status: Annotated[TenantStatus | None, Query(alias="status")] = None,
    slug: str | None = None,
) -> TenantListDTO:
    command = ListTenantsCommand(limit=limit, cursor=cursor, status=tenant_status, slug=slug)
    return await Mediator().send(command, session=session)


@router.post("", response_model=TenantWithTaskDTO, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    command: CreateTenantCommand,
    response: Response,
    session: SessionDep,
) -> TenantWithTaskDTO:
    result = await Mediator().send(command, session=session)
    response.headers["Location"] = f"/v1/tenants/{result.tenant.id}"
    return result


@router.get("/by-slug/{slug}", response_model=TenantDTO)
async def get_tenant_by_slug(slug: str, session: SessionDep) -> TenantDTO:
    return await Mediator().send(GetTenantBySlugCommand(slug=slug), session=session)


@router.get("/{tenant_id}", response_model=TenantDTO)
async def get_tenant(tenant_id: UUID, session: SessionDep) -> TenantDTO:
    return await Mediator().send(GetTenantCommand(tenant_id=tenant_id), session=session)


@router.patch(
    "/{tenant_id}", response_model=TenantWithTaskDTO, status_code=status.HTTP_202_ACCEPTED
)
async def update_tenant(
    tenant_id: UUID,
    body: UpdateTenantBody,
    response: Response,
    session: SessionDep,
) -> TenantWithTaskDTO:
    command = UpdateTenantCommand(tenant_id=tenant_id, **body.model_dump())
    result = await Mediator().send(command, session=session)
    response.headers["Location"] = f"/v1/tasks/{result.task.id}"
    return result


@router.delete(
    "/{tenant_id}", response_model=TenantWithTaskDTO, status_code=status.HTTP_202_ACCEPTED
)
async def delete_tenant(
    tenant_id: UUID,
    response: Response,
    session: SessionDep,
) -> TenantWithTaskDTO:
    result = await Mediator().send(DeleteTenantCommand(tenant_id=tenant_id), session=session)
    response.headers["Location"] = f"/v1/tasks/{result.task.id}"
    return result


@router.get("/{tenant_id}/tasks", response_model=TaskListDTO, tags=["Tasks"])
async def list_tenant_tasks(
    tenant_id: UUID,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
) -> TaskListDTO:
    command = ListTenantTasksCommand(tenant_id=tenant_id, limit=limit, cursor=cursor)
    return await Mediator().send(command, session=session)
