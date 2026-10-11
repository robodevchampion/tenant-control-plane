from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from cp.config import get_settings

settings = get_settings()
engine = create_async_engine(settings.database_url, pool_pre_ping=True, connect_args={"timeout": 3})
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session
