import asyncio
import json
import logging
from datetime import UTC, datetime

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractRobustConnection
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cp.config import Settings, get_settings
from cp.db.models import Outbox
from cp.db.session import SessionFactory
from function.publisher.connection import connect_broker
from function.publisher.topology import declare_topology

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = SessionFactory,
        settings: Settings | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings or get_settings()

    async def publish_batch(self, exchange: AbstractExchange) -> list[tuple[str, str]]:
        async with self.session_factory() as session:
            async with session.begin():
                result = await session.scalars(
                    select(Outbox)
                    .where(Outbox.published_at.is_(None))
                    .order_by(Outbox.id)
                    .limit(self.settings.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
                events = list(result.all())
                published_events = []
                for event in events:
                    message = aio_pika.Message(
                        body=json.dumps(event.payload, separators=(",", ":")).encode(),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=str(event.task_id),
                        timestamp=event.created_at,
                    )
                    await exchange.publish(
                        message,
                        routing_key=event.routing_key,
                        mandatory=True,
                    )
                    event.published_at = datetime.now(UTC)
                    published_events.append((str(event.task_id), event.routing_key))
                return published_events

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff_seconds = 0.5
        logger.info(
            "Outbox publisher starting poll_interval_ms=%s batch_size=%s",
            self.settings.outbox_poll_interval_ms,
            self.settings.outbox_batch_size,
        )
        while not stop_event.is_set():
            connection: AbstractRobustConnection | None = None
            try:
                connection = await connect_broker(self.settings.broker_url)
                channel = await connection.channel(publisher_confirms=True)
                topology = await declare_topology(channel)
                logger.info("Outbox publisher connected and topology declared")
                backoff_seconds = 0.5

                while not stop_event.is_set():
                    published_events = await self.publish_batch(topology.tasks_exchange)
                    if published_events:
                        for task_id, routing_key in published_events:
                            logger.info(
                                "Published task event task_id=%s routing_key=%s",
                                task_id,
                                routing_key,
                            )
                        continue
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self.settings.outbox_poll_interval_ms / 1000,
                        )
                    except TimeoutError:
                        pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox publisher failed; reconnecting")
            finally:
                if connection is not None and not connection.is_closed:
                    try:
                        await connection.close()
                    except Exception:
                        logger.exception("Failed to close RabbitMQ connection")

            if not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff_seconds)
                except TimeoutError:
                    backoff_seconds = min(backoff_seconds * 2, 10)
            logger.info("Outbox publisher stopped")
