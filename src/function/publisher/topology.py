import aio_pika
from aio_pika.abc import AbstractExchange, AbstractRobustChannel

TASKS_EXCHANGE = "cp.tasks"
TASKS_QUEUE = "cp.tasks.worker"
TASK_UPDATE_EXCHANGE = "cp.task-updates"
TASK_UPDATES_QUEUE = "cp.task-updates"
DLX_EXCHANGE = "cp.dlx"
TASK_UPDATES_DLQ = "cp.task-updates.dlq"
TASK_ROUTING_KEYS = ("task.deploy", "task.update", "task.destroy")
TASK_UPDATE_ROUTING_KEY = "task.update"
TASK_UPDATES_DEAD_ROUTING_KEY = "task-updates.dead"


async def declare_topology(channel: AbstractRobustChannel) -> AbstractExchange:
    tasks_exchange = await channel.declare_exchange(
        TASKS_EXCHANGE,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    task_queue = await channel.declare_queue(TASKS_QUEUE, durable=True)
    for routing_key in TASK_ROUTING_KEYS:
        await task_queue.bind(tasks_exchange, routing_key=routing_key)

    updates_exchange = await channel.declare_exchange(
        TASK_UPDATE_EXCHANGE,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_exchange = await channel.declare_exchange(
        DLX_EXCHANGE,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    updates_queue = await channel.declare_queue(
        TASK_UPDATES_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": TASK_UPDATES_DEAD_ROUTING_KEY,
        },
    )
    await updates_queue.bind(updates_exchange, routing_key=TASK_UPDATE_ROUTING_KEY)

    dead_letter_queue = await channel.declare_queue(TASK_UPDATES_DLQ, durable=True)
    await dead_letter_queue.bind(
        dead_letter_exchange,
        routing_key=TASK_UPDATES_DEAD_ROUTING_KEY,
    )
    return tasks_exchange
