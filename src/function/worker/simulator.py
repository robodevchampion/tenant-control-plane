import asyncio
import json
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage, AbstractRobustConnection
from pydantic import ValidationError

from cp.config import Settings, get_settings
from function.publisher.connection import connect_broker
from function.publisher.topology import TASKS_QUEUE, declare_topology
from function.worker.contracts import TaskEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerOptions:
    fail_rate: float
    min_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    retry_backoff_ms: int
    concurrency: int
    duplicate_rate: float
    seed: int | None
    queue_name: str = TASKS_QUEUE

    @property
    def attempt_failure_rate(self) -> float:
        # fail_rate is evaluated once per task, not once per retry attempt.
        # This keeps the contract that fail_rate=0.2 means roughly 20% of tasks fail.
        return 1.0

    def __post_init__(self) -> None:
        if not 0 <= self.fail_rate <= 1:
            raise ValueError("fail_rate must be between 0 and 1")
        if not 0 <= self.duplicate_rate <= 1:
            raise ValueError("duplicate_rate must be between 0 and 1")
        if self.min_delay_ms < 0 or self.max_delay_ms < self.min_delay_ms:
            raise ValueError("delay range must satisfy 0 <= min_delay_ms <= max_delay_ms")
        if self.max_attempts < 1 or self.concurrency < 1 or self.retry_backoff_ms < 0:
            raise ValueError("attempts/concurrency must be positive and backoff non-negative")
        if not self.queue_name:
            raise ValueError("queue_name must not be empty")

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        *,
        queue_name: str = TASKS_QUEUE,
    ) -> "WorkerOptions":
        settings = settings or get_settings()
        return cls(
            fail_rate=settings.worker_fail_rate,
            min_delay_ms=settings.worker_min_delay_ms,
            max_delay_ms=settings.worker_max_delay_ms,
            max_attempts=settings.worker_max_attempts,
            retry_backoff_ms=settings.worker_retry_backoff_ms,
            concurrency=settings.worker_concurrency,
            duplicate_rate=settings.worker_duplicate_rate,
            seed=settings.worker_seed,
            queue_name=queue_name,
        )


class WorkerSimulator:
    def __init__(
        self,
        options: WorkerOptions | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.options = options or WorkerOptions.from_settings(self.settings)
        self.random = random.Random(self.options.seed)  # nosec B311
        self.running_tasks: set[UUID] = set()
        self.terminal_updates: dict[UUID, dict[str, str | None]] = {}
        self.state_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(self.options.concurrency)

    @staticmethod
    def _update_payload(
        task_id: UUID,
        status: str,
        error: str | None = None,
    ) -> dict[str, str | None]:
        update_id = uuid5(NAMESPACE_URL, f"{task_id}:{status}")
        return {
            "update_id": str(update_id),
            "task_id": str(task_id),
            "status": status,
            "error": error,
            "occurred_at": datetime.now(UTC).isoformat(),
        }

    async def _publish_update(
        self,
        exchange: AbstractExchange,
        payload: dict[str, str | None],
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        update_id = payload["update_id"]
        task_id = payload["task_id"]
        await exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=update_id,
            ),
            routing_key="task.update",
            mandatory=True,
        )
        logger.info(
            "Published worker update update_id=%s task_id=%s status=%s",
            update_id,
            task_id,
            payload["status"],
        )
        if self.random.random() < self.options.duplicate_rate:
            await exchange.publish(
                aio_pika.Message(
                    body=body,
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=update_id,
                ),
                routing_key="task.update",
                mandatory=True,
            )
            logger.info("Republished duplicate worker update update_id=%s", update_id)

    async def _simulate_outcome(self, task_id: UUID) -> tuple[str, str | None]:
        if self.random.random() >= self.options.fail_rate:
            delay_ms = self.random.randint(self.options.min_delay_ms, self.options.max_delay_ms)
            logger.info(
                "Applying simulated worker delay task_id=%s attempt=1/1 delay_ms=%s",
                task_id,
                delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000)
            return "done", None
        last_error = "Simulated provisioning failure"
        for attempt in range(1, self.options.max_attempts + 1):
            delay_ms = self.random.randint(self.options.min_delay_ms, self.options.max_delay_ms)
            logger.info(
                "Applying simulated worker delay task_id=%s attempt=%s/%s delay_ms=%s",
                task_id,
                attempt,
                self.options.max_attempts,
                delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000)
            last_error = f"Simulated provisioning failure on attempt {attempt}"
            if attempt < self.options.max_attempts:
                base_delay = self.options.retry_backoff_ms * (2 ** (attempt - 1))
                retry_delay = base_delay * self.random.uniform(0.75, 1.25)
                logger.warning(
                    "Simulated attempt failed task_id=%s attempt=%s/%s retry_delay_ms=%.0f",
                    task_id,
                    attempt,
                    self.options.max_attempts,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay / 1000)
            logger.error(
                "Worker exhausted attempts task_id=%s attempts=%s terminal_status=failed error=%s",
                task_id,
                self.options.max_attempts,
                last_error,
            )
        return "failed", last_error

    async def _handle_message(
        self,
        message: AbstractIncomingMessage,
        update_exchange: AbstractExchange,
    ) -> None:
        try:
            task_event = TaskEvent.model_validate_json(message.body)
        except (ValidationError, ValueError) as exc:
            logger.exception("Rejecting invalid task message: %s", exc)
            await message.reject(requeue=False)
            return

        task_id = task_event.id
        logger.info(
            "Received task event task_id=%s type=%s status=%s queue=%s",
            task_id,
            task_event.type.value,
            task_event.status.value,
            self.options.queue_name,
        )
        async with self.semaphore:
            async with self.state_lock:
                terminal_update = self.terminal_updates.get(task_id)
                if terminal_update is not None:
                    pass
                elif task_id in self.running_tasks:
                    await message.ack()
                    logger.info("Acknowledged duplicate in-progress task task_id=%s", task_id)
                    return
                else:
                    self.running_tasks.add(task_id)

            if terminal_update is not None:
                logger.info(
                    "Replaying stored terminal state task_id=%s status=%s",
                    task_id,
                    terminal_update["status"],
                )
                await self._publish_update(update_exchange, terminal_update)
                await message.ack()
                logger.info("Replayed stored terminal result for task_id=%s", task_id)
                return

            try:
                progress_update = self._update_payload(task_id, "in_progress")
                logger.info(
                    "Publishing in_progress task_id=%s update_id=%s",
                    task_id,
                    progress_update["update_id"],
                )
                await self._publish_update(update_exchange, progress_update)
                status, error = await self._simulate_outcome(task_id)
                terminal_update = self._update_payload(task_id, status, error)
                async with self.state_lock:
                    self.terminal_updates[task_id] = terminal_update
                logger.info(
                    "Terminal outcome decided task_id=%s status=%s error=%s", task_id, status, error
                )
                await self._publish_update(update_exchange, terminal_update)
                await message.ack()
                logger.info(
                    "Completed simulated task task_id=%s type=%s status=%s",
                    task_id,
                    task_event.type.value,
                    status,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker failed task_id=%s; requeueing task", task_id)
                await asyncio.sleep(0.2)
                await message.nack(requeue=True)
            finally:
                async with self.state_lock:
                    self.running_tasks.discard(task_id)

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff_seconds = 0.5
        logger.info(
            "Worker starting queue=%s concurrency=%s max_attempts=%s "
            "fail_rate=%.3f min_delay_ms=%s max_delay_ms=%s",
            self.options.queue_name,
            self.options.concurrency,
            self.options.max_attempts,
            self.options.fail_rate,
            self.options.min_delay_ms,
            self.options.max_delay_ms,
        )
        while not stop_event.is_set():
            connection: AbstractRobustConnection | None = None
            consumer_tag: str | None = None
            queue = None
            try:
                connection = await connect_broker(self.settings.broker_url)
                channel = await connection.channel(publisher_confirms=True)
                await channel.set_qos(prefetch_count=self.options.concurrency)
                topology = await declare_topology(
                    channel,
                    tasks_queue_name=self.options.queue_name,
                )
                queue = topology.tasks_queue

                async def handle_message(message: AbstractIncomingMessage) -> None:
                    await self._handle_message(message, topology.task_updates_exchange)

                consumer_tag = await queue.consume(handle_message)
                logger.info("Worker listening on queue=%s", queue.name)
                backoff_seconds = 0.5
                await stop_event.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker connection failed; reconnecting")
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
                        logger.exception("Failed to cancel worker consumer")
                if connection is not None and not connection.is_closed:
                    try:
                        await connection.close()
                    except Exception:
                        logger.exception("Failed to close worker connection")

            if not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff_seconds)
                except TimeoutError:
                    backoff_seconds = min(backoff_seconds * 2, 10)
        logger.info("Worker stopped")
