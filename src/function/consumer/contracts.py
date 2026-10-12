from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator

WorkerStatus = Literal["in_progress", "done", "failed"]


class WorkerUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    update_id: UUID
    task_id: UUID
    status: WorkerStatus
    error: str | None = None
    occurred_at: datetime

    @model_validator(mode="after")
    def validate_update(self) -> "WorkerUpdate":
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must include a timezone")
        if self.status == "failed" and not self.error:
            raise ValueError("error is required when status is failed")
        if self.status != "failed" and self.error is not None:
            raise ValueError("error is only allowed when status is failed")
        return self
