import logging
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from function.worker.__main__ import parse_failure_rate
from function.worker.contracts import TaskEvent
from function.worker.simulator import WorkerOptions, WorkerSimulator


def make_options(**overrides: object) -> WorkerOptions:
    values: dict[str, object] = {
        "fail_rate": 0.0,
        "min_delay_ms": 0,
        "max_delay_ms": 0,
        "max_attempts": 1,
        "retry_backoff_ms": 0,
        "concurrency": 1,
        "duplicate_rate": 0.0,
        "seed": 11,
    }
    values.update(overrides)
    return WorkerOptions(**values)


class FakeExchange:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, message, routing_key: str, mandatory: bool) -> None:
        self.messages.append((message, routing_key, mandatory))


class FakeIncomingMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.ack_count = 0
        self.reject_calls = []

    async def ack(self) -> None:
        self.ack_count += 1

    async def reject(self, requeue: bool = False) -> None:
        self.reject_calls.append(requeue)


def make_task_event(task_id) -> bytes:
    return (
        TaskEvent(
            id=task_id,
            type="deploy",
            tenant_id=uuid4(),
            status="accepted",
            created_at=datetime.now(UTC),
        )
        .model_dump_json()
        .encode()
    )


@pytest.mark.asyncio
async def test_worker_simulates_success_and_exhausted_failure(caplog) -> None:
    caplog.set_level(logging.INFO)
    successful = WorkerSimulator(make_options(fail_rate=0.0))
    failed = WorkerSimulator(make_options(fail_rate=1.0, max_attempts=3))

    assert await successful._simulate_outcome(uuid4()) == ("done", None)
    outcome, error = await failed._simulate_outcome(uuid4())
    assert outcome == "failed"
    assert "attempt 3" in error
    assert "Applying simulated worker delay" in caplog.text
    assert "delay_ms=0" in caplog.text
    assert "Worker exhausted attempts" in caplog.text


def test_fail_rate_is_task_level_and_parses_ratio_notation() -> None:
    assert parse_failure_rate("1/10") == pytest.approx(0.1)
    options = WorkerOptions(
        fail_rate=0.1,
        min_delay_ms=0,
        max_delay_ms=0,
        max_attempts=3,
        retry_backoff_ms=0,
        concurrency=1,
        duplicate_rate=0,
        seed=4,
    )
    assert options.fail_rate == pytest.approx(0.1)
    assert options.attempt_failure_rate == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_fail_rate_approximates_requested_fraction_of_tasks() -> None:
    worker = WorkerSimulator(make_options(fail_rate=0.2, max_attempts=5, seed=71))

    results = [await worker._simulate_outcome(uuid4()) for _ in range(500)]
    failure_count = sum(status == "failed" for status, _ in results)

    assert 70 <= failure_count <= 130


def test_worker_update_ids_are_deterministic_by_task_and_status() -> None:
    task_id = uuid4()

    first = WorkerSimulator._update_payload(task_id, "done")
    repeated = WorkerSimulator._update_payload(task_id, "done")
    progress = WorkerSimulator._update_payload(task_id, "in_progress")

    assert first["update_id"] == repeated["update_id"]
    assert first["update_id"] != progress["update_id"]


@pytest.mark.asyncio
async def test_redelivered_task_republishes_identical_terminal_update() -> None:
    worker = WorkerSimulator(make_options())
    task_id = uuid4()
    exchange = FakeExchange()

    await worker._handle_message(FakeIncomingMessage(make_task_event(task_id)), exchange)
    await worker._handle_message(FakeIncomingMessage(make_task_event(task_id)), exchange)

    assert len(exchange.messages) == 3
    assert exchange.messages[1][0].body == exchange.messages[2][0].body
    assert exchange.messages[1][0].message_id == exchange.messages[2][0].message_id
