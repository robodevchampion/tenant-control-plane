from uuid import uuid4

import pytest
from pydantic import ValidationError

from function.consumer.contracts import WorkerUpdate


def make_update(**overrides: object) -> dict[str, object]:
    update: dict[str, object] = {
        "update_id": str(uuid4()),
        "task_id": str(uuid4()),
        "status": "in_progress",
        "error": None,
        "occurred_at": "2026-10-12T10:15:30Z",
    }
    update.update(overrides)
    return update


def test_worker_update_validates_contract_fields() -> None:
    update = WorkerUpdate.model_validate(make_update())

    assert update.status == "in_progress"
    assert update.error is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "accepted"},
        {"status": "failed"},
        {"status": "done", "error": "unexpected"},
        {"occurred_at": "2026-10-12T10:15:30"},
        {"unexpected": True},
    ],
)
def test_worker_update_rejects_invalid_contract_fields(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WorkerUpdate.model_validate(make_update(**overrides))
