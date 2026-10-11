from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from cp.application.commands.task_commands import GetTaskCommand, ListTasksCommand
from cp.application.core.mediator import Mediator
from cp.application.dto import TaskDTO, TaskListDTO
from cp.db.session import get_session
from cp.domain.enums import TaskStatus, TaskType

router = APIRouter(prefix="/v1/tasks", tags=["Tasks"])
SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("", response_model=TaskListDTO)
async def list_tasks(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
    tenant_id: UUID | None = None,
    task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    task_type: Annotated[TaskType | None, Query(alias="type")] = None,
) -> TaskListDTO:
    command = ListTasksCommand(
        limit=limit,
        cursor=cursor,
        tenant_id=tenant_id,
        status=task_status,
        task_type=task_type,
    )
    return await Mediator().send(command, session=session)


@router.get("/{task_id}", response_model=TaskDTO)
async def get_task(task_id: UUID, session: SessionDep) -> TaskDTO:
    return await Mediator().send(GetTaskCommand(task_id=task_id), session=session)
