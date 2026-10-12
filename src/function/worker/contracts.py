from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator

from cp.domain.enums import TaskStatus, TaskType


class TaskEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: datetime

    @model_validator(mode="after")
    def validate_accepted_status(self) -> "TaskEvent":
        if self.status != TaskStatus.ACCEPTED:
            raise ValueError("worker accepts only tasks in accepted status")
        return self
