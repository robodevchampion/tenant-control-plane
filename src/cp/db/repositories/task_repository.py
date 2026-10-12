from datetime import datetime
from uuid import UUID

from sqlalchemy import select

from cp.db.models import Task
from cp.db.repositories.base_repository import BaseRepository
from cp.domain.enums import TaskStatus


class TaskRepository(BaseRepository):
    async def add_task(self, task: Task) -> Task:
        return await self.add(task)

    async def get_for_update(self, task_id: UUID) -> Task | None:
        result = await self.session.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def apply_worker_update(
        self, task: Task, status: TaskStatus, error: str | None, now: datetime
    ) -> None:
        task.status = status
        task.error = error
        task.updated_at = now
        await self.session.flush()
