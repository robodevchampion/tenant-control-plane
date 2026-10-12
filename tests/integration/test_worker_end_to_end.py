import asyncio
import json
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
from function.publisher.topology import TASKS_EXCHANGE, TASK_UPDATE_ROUTING_KEY
from function.worker.simulator import WorkerOptions, WorkerSimulator


async def wait_for(check, description: str, timeout: float = 15) -> None:
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.05)


async def receive_task_event(queue, task_id: UUID) -> dict[str, object]:
    deferred = []
    try:
        async with asyncio.timeout(15):
            while True:
                message = await queue.get(timeout=1, fail=False)
                if message is None:
                    continue
                if message.message_id == str(task_id):
                    await message.ack()
                    return json.loads(message.body)
                deferred.append(message)
    finally:
        for message in deferred:
            await message.nack(requeue=True)


@pytest.mark.asyncio
async def test_worker_fail_rate_is_task_level() -> None:
    worker = WorkerSimulator(
        WorkerOptions(
            fail_rate=0.2,
            min_delay_ms=0,
            max_delay_ms=0,
            max_attempts=5,
            retry_backoff_ms=0,
            concurrency=1,
            duplicate_rate=0.0,
            seed=71,
        )
    )

    outcomes = [await worker._simulate_outcome(uuid4()) for _ in range(500)]
    failure_count = sum(status == "failed" for status, _ in outcomes)

    assert 70 <= failure_count <= 130


@pytest.mark.asyncio
async def test_worker_completes_tenant_through_api_publisher_and_consumer() -> None:
    try:
        connection = await aio_pika.connect_robust(get_settings().broker_url, timeout=5)
        async with engine.connect() as db_connection:
            await db_connection.execute(select(1))
    except Exception as exc:
        pytest.skip(f"PostgreSQL and RabbitMQ are required for this integration test: {exc}")

    tenant_id: UUID | None = None
    task_id: UUID | None = None
    worker_stop = asyncio.Event()
    worker_task: asyncio.Task | None = None
    try:
        channel = await connection.channel()
        tasks_exchange = await channel.get_exchange(TASKS_EXCHANGE, ensure=True)
        observer = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await observer.bind(tasks_exchange, routing_key="task.deploy")

        worker = WorkerSimulator(
            WorkerOptions(
                fail_rate=0.0,
                min_delay_ms=0,
                max_delay_ms=0,
                max_attempts=1,
                retry_backoff_ms=0,
                concurrency=1,
                duplicate_rate=0.0,
                seed=7,
            )
        )
        worker_task = asyncio.create_task(worker.run(worker_stop), name="test-worker")

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
                        "slug": f"worker-e2e-{uuid4().hex[:10]}",
                        "name": "Worker end-to-end test",
                    },
                )
            assert response.status_code == 201, response.text
            body = response.json()
            tenant_id = UUID(body["tenant"]["id"])
            task_id = UUID(body["task"]["id"])
            assert body["tenant"]["status"] == "provisioning"
            assert body["task"]["status"] == "accepted"

            task_event = await receive_task_event(observer, task_id)
            assert task_event["id"] == str(task_id)
            assert task_event["tenant_id"] == str(tenant_id)
            assert task_event["type"] == "deploy"

            async def lifecycle_finished() -> bool:
                async with SessionFactory() as session:
                    tenant = await session.get(Tenant, tenant_id)
                    task = await session.get(Task, task_id)
                    outbox = await session.scalar(
                        select(Outbox).where(Outbox.task_id == task_id)
                    )
                    updates = await session.scalar(
                        select(func.count())
                        .select_from(ProcessedUpdate)
                        .where(ProcessedUpdate.task_id == task_id)
                    )
                    return (
                        tenant is not None
                        and tenant.status == TenantStatus.ACTIVE
                        and tenant.version == 2
                        and task is not None
                        and task.status == TaskStatus.DONE
                        and outbox is not None
                        and outbox.published_at is not None
                        and updates == 2
                    )

            await wait_for(lifecycle_finished, "tenant active, task done, event published, 2 updates recorded")

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                tenant_response = await client.get(f"/v1/tenants/{tenant_id}")
                task_response = await client.get(f"/v1/tasks/{task_id}")
            assert tenant_response.json()["status"] == "active"
            assert tenant_response.json()["version"] == 2
            assert task_response.json()["status"] == "done"
            assert task_response.json()["type"] == "deploy"
    finally:
        worker_stop.set()
        if worker_task is not None:
            try:
                await asyncio.wait_for(worker_task, timeout=5)
            except TimeoutError:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
        if task_id is not None and tenant_id is not None:
            async with SessionFactory() as session:
                async with session.begin():
                    await session.execute(
                        delete(ProcessedUpdate).where(ProcessedUpdate.task_id == task_id)
                    )
                    await session.execute(delete(Outbox).where(Outbox.task_id == task_id))
                    await session.execute(delete(Task).where(Task.id == task_id))
                    await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
        await connection.close()
        await engine.dispose()