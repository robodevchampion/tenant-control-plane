import asyncio
import json
from uuid import uuid4

import aio_pika
import httpx
import pytest
from sqlalchemy import delete, select

from cp.config import get_settings
from cp.db.models import Outbox, Task, Tenant
from cp.db.session import SessionFactory, engine
from cp.main import create_app
from function.publisher.topology import TASKS_EXCHANGE, TASKS_QUEUE


async def receive_matching(queue, message_id: str):
    deferred = []
    try:
        async with asyncio.timeout(10):
            while True:
                message = await queue.get(timeout=1, fail=False)
                if message is None:
                    continue
                if message.message_id == message_id:
                    await message.ack()
                    return message
                deferred.append(message)
    finally:
        for message in deferred:
            await message.nack(requeue=True)


@pytest.mark.asyncio
async def test_lifespan_publishes_outbox_event_to_worker_queue() -> None:
    try:
        connection = await aio_pika.connect_robust(get_settings().broker_url, timeout=5)
        async with engine.connect() as db_connection:
            await db_connection.execute(select(1))
    except Exception as exc:
        pytest.skip(f"PostgreSQL and RabbitMQ are required for this integration test: {exc}")

    tenant_id = None
    task_id = None
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            TASKS_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        observer_queue = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await observer_queue.bind(exchange, routing_key="task.deploy")
        worker_queue = await channel.declare_queue(TASKS_QUEUE, durable=True)

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
                        "slug": f"publisher-{uuid4().hex[:12]}",
                        "name": "Publisher integration test",
                    },
                )
            assert response.status_code == 201, response.text
            result = response.json()
            tenant_id = result["tenant"]["id"]
            task_id = result["task"]["id"]

            observed = await receive_matching(observer_queue, task_id)
            assert observed.content_type == "application/json"
            assert observed.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
            assert json.loads(observed.body)["id"] == task_id

            delivered = await receive_matching(worker_queue, task_id)
            assert json.loads(delivered.body)["tenant_id"] == tenant_id

            async with SessionFactory() as session:
                event = await session.scalar(select(Outbox).where(Outbox.task_id == task_id))
                assert event is not None
                assert event.published_at is not None
    finally:
        if tenant_id and task_id:
            async with SessionFactory() as session:
                async with session.begin():
                    await session.execute(delete(Outbox).where(Outbox.task_id == task_id))
                    await session.execute(delete(Task).where(Task.id == task_id))
                    await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
        await connection.close()
        await engine.dispose()
