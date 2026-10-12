import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import aio_pika
import httpx
import pytest
from sqlalchemy import delete, func, select

from cp.config import get_settings
from cp.db.models import Outbox, ProcessedUpdate, Task, Tenant
from cp.db.session import SessionFactory, engine
from cp.domain.enums import TaskStatus, TenantStatus
from cp.main import create_app
from function.publisher.topology import (
    TASK_UPDATE_EXCHANGE,
    TASK_UPDATE_ROUTING_KEY,
    TASK_UPDATES_DEAD_ROUTING_KEY,
    TASK_UPDATES_DLQ,
    TASKS_QUEUE,
    declare_topology,
)


async def wait_until(check, timeout: float = 10) -> None:
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.05)


async def publish_update(exchange, task_id: UUID, status: str, error: str | None = None):
    update_id = uuid4()
    payload = {
        "update_id": str(update_id),
        "task_id": str(task_id),
        "status": status,
        "error": error,
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(update_id),
        ),
        routing_key=TASK_UPDATE_ROUTING_KEY,
        mandatory=True,
    )
    return update_id


async def receive_task_message(queue, task_id: UUID):
    deferred = []
    try:
        async with asyncio.timeout(10):
            while True:
                message = await queue.get(timeout=1, fail=False)
                if message is None:
                    continue
                if message.message_id == str(task_id):
                    await message.ack()
                    return message
                deferred.append(message)
    finally:
        for message in deferred:
            await message.nack(requeue=True)


async def receive_dead_letter(queue, expected_body: bytes):
    deferred = []
    try:
        async with asyncio.timeout(10):
            while True:
                message = await queue.get(fail=False)
                if message is None:
                    await asyncio.sleep(0.05)
                    continue
                if message.body == expected_body:
                    await message.ack()
                    return message
                deferred.append(message)
    finally:
        for message in deferred:
            await message.nack(requeue=True)


async def test_consumer_applies_updates_and_dead_letters_invalid_messages() -> None:
    try:
        connection = await aio_pika.connect_robust(get_settings().broker_url, timeout=5)
        async with engine.connect() as db_connection:
            await db_connection.execute(select(1))
    except Exception as exc:
        pytest.skip(f"PostgreSQL and RabbitMQ are required for this integration test: {exc}")

    tenant_id: UUID | None = None
    task_id: UUID | None = None
    try:
        channel = await connection.channel(publisher_confirms=True)
        topology = await declare_topology(channel)
        task_updates_exchange = await channel.get_exchange(TASK_UPDATE_EXCHANGE, ensure=True)
        task_queue = await channel.get_queue(TASKS_QUEUE, ensure=True)
        dead_letter_queue = await channel.get_queue(TASK_UPDATES_DLQ, ensure=True)

        app = create_app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                timeout=10,
            ) as client:
                response = await client.post(
                    "/v1/tenants",
                    json={
                        "slug": f"consumer-{uuid4().hex[:12]}",
                        "name": "Consumer integration test",
                    },
                )
            assert response.status_code == 201, response.text
            result = response.json()
            tenant_id = UUID(result["tenant"]["id"])
            task_id = UUID(result["task"]["id"])

            async def consumer_is_attached() -> bool:
                declaration = await topology.task_updates_queue.declare()
                return declaration.consumer_count > 0

            await wait_until(consumer_is_attached)

            progress_update_id = await publish_update(
                task_updates_exchange,
                task_id,
                "in_progress",
            )

            async def task_is_in_progress() -> bool:
                async with SessionFactory() as session:
                    task = await session.get(Task, task_id)
                    return task is not None and task.status == TaskStatus.IN_PROGRESS

            await wait_until(task_is_in_progress)

            done_update_id = await publish_update(task_updates_exchange, task_id, "done")

            async def task_and_tenant_are_complete() -> bool:
                async with SessionFactory() as session:
                    task = await session.get(Task, task_id)
                    tenant = await session.get(Tenant, tenant_id)
                    return (
                        task is not None
                        and task.status == TaskStatus.DONE
                        and tenant is not None
                        and tenant.status == TenantStatus.ACTIVE
                        and tenant.version == 1
                    )

            await wait_until(task_and_tenant_are_complete)

            await task_updates_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(
                        {
                            "update_id": str(done_update_id),
                            "task_id": str(task_id),
                            "status": "done",
                            "error": None,
                            "occurred_at": datetime.now(UTC).isoformat(),
                        }
                    ).encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(done_update_id),
                ),
                routing_key=TASK_UPDATE_ROUTING_KEY,
                mandatory=True,
            )
            stale_update_id = await publish_update(
                task_updates_exchange,
                task_id,
                "in_progress",
            )

            async def duplicate_and_stale_are_recorded_once() -> bool:
                async with SessionFactory() as session:
                    count = await session.scalar(
                        select(func.count())
                        .select_from(ProcessedUpdate)
                        .where(ProcessedUpdate.task_id == task_id)
                    )
                    return count == 3

            await wait_until(duplicate_and_stale_are_recorded_once)
            async with SessionFactory() as session:
                task = await session.get(Task, task_id)
                update_ids = set(
                    (
                        await session.scalars(
                            select(ProcessedUpdate.update_id).where(
                                ProcessedUpdate.task_id == task_id
                            )
                        )
                    ).all()
                )
            assert task is not None and task.status == TaskStatus.DONE
            assert update_ids == {progress_update_id, done_update_id, stale_update_id}

            poison = f"not-json-{uuid4()}".encode()
            await task_updates_exchange.publish(
                aio_pika.Message(body=poison, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=TASK_UPDATE_ROUTING_KEY,
                mandatory=True,
            )
            dead_letter = await receive_dead_letter(dead_letter_queue, poison)
            assert dead_letter.routing_key == TASK_UPDATES_DEAD_ROUTING_KEY

            unknown_task_id = uuid4()
            unknown_update = {
                "update_id": str(uuid4()),
                "task_id": str(unknown_task_id),
                "status": "in_progress",
                "error": None,
                "occurred_at": datetime.now(UTC).isoformat(),
            }
            unknown_body = json.dumps(unknown_update).encode()
            await task_updates_exchange.publish(
                aio_pika.Message(
                    body=unknown_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=TASK_UPDATE_ROUTING_KEY,
                mandatory=True,
            )
            dead_letter = await receive_dead_letter(dead_letter_queue, unknown_body)
            assert dead_letter.routing_key == TASK_UPDATES_DEAD_ROUTING_KEY

            worker_message = await receive_task_message(task_queue, task_id)
            assert json.loads(worker_message.body)["tenant_id"] == str(tenant_id)
    finally:
        try:
            if task_id is not None and tenant_id is not None:
                async with SessionFactory() as session:
                    async with session.begin():
                        await session.execute(
                            delete(ProcessedUpdate).where(ProcessedUpdate.task_id == task_id)
                        )
                        await session.execute(delete(Outbox).where(Outbox.task_id == task_id))
                        await session.execute(delete(Task).where(Task.id == task_id))
                        await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
        finally:
            await connection.close()
            await engine.dispose()
