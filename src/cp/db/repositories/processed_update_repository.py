from cp.db.models import ProcessedUpdate
from cp.db.repositories.base_repository import BaseRepository


class ProcessedUpdateRepository(BaseRepository):
    async def add_update(self, update: ProcessedUpdate) -> ProcessedUpdate:
        return await self.add(update)
