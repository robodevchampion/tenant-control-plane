import asyncio

import aio_pika
from sqlalchemy import text

from cp.application.commands.health_commands import LivenessCommand, ReadinessCommand
from cp.application.core.mediator import Mediator
from cp.config import get_settings
from cp.db.session import engine


@Mediator.register_handler(LivenessCommand)
class LivenessHandler:
    async def handle(self, command: LivenessCommand) -> dict[str, str]:
        return {"status": "ok"}


@Mediator.register_handler(ReadinessCommand)
class ReadinessHandler:
    async def handle(self, command: ReadinessCommand) -> dict[str, object]:
        checks = {"database": "unavailable", "broker": "unavailable"}
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            checks["database"] = "ok"
        except Exception:
            checks["database"] = "unavailable"

        try:
            async with asyncio.timeout(3):
                connection = await aio_pika.connect_robust(get_settings().broker_url)
                await connection.close()
            checks["broker"] = "ok"
        except Exception:
            checks["broker"] = "unavailable"

        ready = all(value == "ok" for value in checks.values())
        return {
            "status": "ok" if ready else "unavailable",
            "checks": checks,
            "http_status": 200 if ready else 503,
        }
