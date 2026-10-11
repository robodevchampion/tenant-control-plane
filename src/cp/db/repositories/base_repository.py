from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from cp.db.base import Base

ModelT = TypeVar("ModelT", bound=Base)


class BaseRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, model: ModelT) -> ModelT:
        self.session.add(model)
        await self.session.flush()
        return model
