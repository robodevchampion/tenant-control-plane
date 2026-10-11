from cp.db.models import Outbox
from cp.db.repositories.base_repository import BaseRepository


class OutboxRepository(BaseRepository):
    async def add_event(self, event: Outbox) -> Outbox:
        return await self.add(event)
