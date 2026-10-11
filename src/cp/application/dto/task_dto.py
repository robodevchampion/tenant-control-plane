from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer

from cp.domain.enums import TaskStatus, TaskType
from cp.serialization import format_timestamp


class TaskDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    error: str | None
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_timestamp(self, value: datetime) -> str:
        return format_timestamp(value)


class TaskListDTO(BaseModel):
    items: list[TaskDTO]
    next_cursor: str | None
