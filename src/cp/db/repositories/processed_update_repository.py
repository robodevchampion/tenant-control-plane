from datetime import datetime
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert

from cp.db.models import ProcessedUpdate
from cp.db.repositories.base_repository import BaseRepository


class ProcessedUpdateRepository(BaseRepository):
    async def add_update(self, update: ProcessedUpdate) -> ProcessedUpdate:
        return await self.add(update)

    async def record_if_new(self, update_id: UUID, task_id: UUID, processed_at: datetime) -> bool:
        statement = (
            insert(ProcessedUpdate)
            .values(update_id=update_id, task_id=task_id, processed_at=processed_at)
            .on_conflict_do_nothing(index_elements=[ProcessedUpdate.update_id])
            .returning(ProcessedUpdate.update_id)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none() is not None
