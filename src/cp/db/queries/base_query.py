from sqlalchemy.ext.asyncio import AsyncSession


class BaseQuery:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
