import asyncio
import json
import logging
import random

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cp.config import Settings, get_settings
from cp.db.session import SessionFactory
from cp.services.task_updates import UnknownTaskUpdate, apply_worker_update
from function.consumer.contracts import WorkerUpdate
from function.publisher.connection import connect_broker
from function.publisher.topology import declare_topology

logger = logging.getLogger(__name__)


class UpdateConsumer:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = SessionFactory,
        settings: Settings | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings or get_settings()

    async def _handle_message(self, message: AbstractIncomingMessage) -> None:
        try:
            payload = json.loads(message.body)
            update = WorkerUpdate.model_validate(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
            logger.warning("Rejecting invalid worker update: %s", exc)
            await message.reject(requeue=False)
            return

        for attempt in range(1, self.settings.consumer_max_attempts + 1):
            try:
                async with self.session_factory() as session:
                    result = await apply_worker_update(session, update)
                await message.ack()
                logger.info(
                    "Acknowledged worker update update_id=%s task_id=%s status=%s outcome=%s",
                    update.update_id,
                    update.task_id,
                    update.status,
                    result,
                )
                return
            except UnknownTaskUpdate as exc:
                logger.warning("Rejecting worker update for unknown task: %s", exc)
                await message.reject(requeue=False)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Worker update %s failed on attempt %s/%s",
                    update.update_id,
                    attempt,
                    self.settings.consumer_max_attempts,
                )
                if attempt == self.settings.consumer_max_attempts:
                    await message.reject(requeue=False)
                    return
                delay = min(0.2 * (2 ** (attempt - 1)), 10)
                await asyncio.sleep(
                    delay + random.uniform(0, delay * 0.25)  # nosec B311
                )

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff_seconds = 0.5
        logger.info(
            "Update consumer starting prefetch=%s max_attempts=%s",
            self.settings.consumer_prefetch,
            self.settings.consumer_max_attempts,
        )
        while not stop_event.is_set():
            connection: AbstractRobustConnection | None = None
            consumer_tag: str | None = None
            queue = None
            try:
                connection = await connect_broker(self.settings.broker_url)
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.settings.consumer_prefetch)
                topology = await declare_topology(channel)
                queue = topology.task_updates_queue
                consumer_tag = await queue.consume(self._handle_message)
                logger.info("Update consumer listening on %s", queue.name)
                backoff_seconds = 0.5
                await stop_event.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Update consumer failed; reconnecting")
            finally:
                if (
                    queue is not None
                    and consumer_tag is not None
                    and connection is not None
                    and not connection.is_closed
                ):
                    try:
                        await queue.cancel(consumer_tag)
                    except Exception:
                        logger.exception("Failed to cancel update consumer")
                if connection is not None and not connection.is_closed:
                    try:
                        await connection.close()
                    except Exception:
                        logger.exception("Failed to close RabbitMQ consumer connection")

            if not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff_seconds)
                except TimeoutError:
                    backoff_seconds = min(backoff_seconds * 2, 10)
        logger.info("Update consumer stopped")
