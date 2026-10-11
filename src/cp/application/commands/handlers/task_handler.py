from sqlalchemy.ext.asyncio import AsyncSession

from cp.application.commands.task_commands import (
    GetTaskCommand,
    ListTasksCommand,
    ListTenantTasksCommand,
)
from cp.application.core.mediator import Mediator
from cp.application.core.pagination import decode_cursor, encode_cursor
from cp.application.dto import TaskDTO, TaskListDTO
from cp.db.queries import TaskQuery, TenantQuery
from cp.domain.errors import DomainError


@Mediator.register_handler(ListTasksCommand)
class ListTasksHandler:
    async def handle(self, command: ListTasksCommand, session: AsyncSession) -> TaskListDTO:
        cursor = decode_cursor(command.cursor) if command.cursor else None
        rows, has_more = await TaskQuery(session).list(
            tenant_id=command.tenant_id,
            status=command.status,
            task_type=command.task_type,
            cursor=cursor,
            limit=command.limit,
        )
        next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id) if has_more else None
        return TaskListDTO(items=rows, next_cursor=next_cursor)


@Mediator.register_handler(ListTenantTasksCommand)
class ListTenantTasksHandler:
    async def handle(self, command: ListTenantTasksCommand, session: AsyncSession) -> TaskListDTO:
        tenant = await TenantQuery(session).get_by_id(command.tenant_id)
        if tenant is None:
            raise DomainError(
                "tenant_not_found",
                f"Tenant {command.tenant_id} was not found.",
                404,
                {"tenant_id": str(command.tenant_id)},
            )

        cursor = decode_cursor(command.cursor) if command.cursor else None
        rows, has_more = await TaskQuery(session).list(
            tenant_id=command.tenant_id,
            status=None,
            task_type=None,
            cursor=cursor,
            limit=command.limit,
        )
        next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id) if has_more else None
        return TaskListDTO(items=rows, next_cursor=next_cursor)


@Mediator.register_handler(GetTaskCommand)
class GetTaskHandler:
    async def handle(self, command: GetTaskCommand, session: AsyncSession) -> TaskDTO:
        task = await TaskQuery(session).get_by_id(command.task_id)
        if task is None:
            raise DomainError(
                "task_not_found",
                f"Task {command.task_id} was not found.",
                404,
                {"task_id": str(command.task_id)},
            )
        return TaskDTO.model_validate(task)
