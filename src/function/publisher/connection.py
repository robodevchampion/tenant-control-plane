import aio_pika
from aio_pika.abc import AbstractRobustConnection


async def connect_broker(url: str) -> AbstractRobustConnection:
    return await aio_pika.connect_robust(url, timeout=5)
