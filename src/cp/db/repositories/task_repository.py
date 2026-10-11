from cp.db.models import Task
from cp.db.repositories.base_repository import BaseRepository


class TaskRepository(BaseRepository):
    async def add_task(self, task: Task) -> Task:
        return await self.add(task)
