import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from cp.application.exceptions import register_error_handlers
from cp.application.middleware import request_logging
from cp.logging_config import configure_application_logging
from function.consumer.consumer import UpdateConsumer
from function.publisher.outbox_publisher import OutboxPublisher
from function.tenant_api import health_router, task_router, tenant_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    stop_event = asyncio.Event()
    publisher_task = asyncio.create_task(
        OutboxPublisher().run(stop_event),
        name="outbox-publisher",
    )
    consumer_task = asyncio.create_task(
        UpdateConsumer().run(stop_event),
        name="task-update-consumer",
    )
    app.state.outbox_publisher_task = publisher_task
    app.state.task_update_consumer_task = consumer_task
    try:
        yield
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(
                asyncio.gather(publisher_task, consumer_task),
                timeout=5,
            )
        except TimeoutError:
            publisher_task.cancel()
            consumer_task.cancel()
            await asyncio.gather(publisher_task, consumer_task, return_exceptions=True)


def create_app() -> FastAPI:
    configure_application_logging()
    app = FastAPI(
        title="Tenant Provisioning Control Plane",
        version="1.0.0",
        lifespan=lifespan,
    )
    register_error_handlers(app)
    app.include_router(health_router)
    app.include_router(tenant_router)
    app.include_router(task_router)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.middleware("http")(request_logging)
    return app
