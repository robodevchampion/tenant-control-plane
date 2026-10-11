from datetime import datetime
from uuid import UUID

from sqlalchemy import select

from cp.db.models import Task
from cp.db.queries.base_query import BaseQuery
from cp.domain.enums import TaskStatus, TaskType


class TaskQuery(BaseQuery):
    async def get_by_id(self, task_id: UUID) -> Task | None:
        return await self.session.get(Task, task_id)

    async def list(
        self,
        *,
        tenant_id: UUID | None,
        status: TaskStatus | None,
        task_type: TaskType | None,
        cursor: tuple[datetime, UUID] | None,
        limit: int,
    ) -> tuple[list[Task], bool]:
        statement = select(Task)
        if tenant_id is not None:
            statement = statement.where(Task.tenant_id == tenant_id)
        if status is not None:
            statement = statement.where(Task.status == status)
        if task_type is not None:
            statement = statement.where(Task.type == task_type)
        if cursor is not None:
            created_at, item_id = cursor
            statement = statement.where(
                (Task.created_at < created_at)
                | ((Task.created_at == created_at) & (Task.id < item_id))
            )
        statement = statement.order_by(Task.created_at.desc(), Task.id.desc()).limit(limit + 1)
        rows = list((await self.session.scalars(statement)).all())
        return rows[:limit], len(rows) > limit
