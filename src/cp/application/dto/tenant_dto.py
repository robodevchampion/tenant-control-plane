from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer

from cp.application.dto.task_dto import TaskDTO
from cp.domain.enums import TenantStatus
from cp.serialization import format_timestamp


class TenantDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_timestamp(self, value: datetime) -> str:
        return format_timestamp(value)


class TenantListDTO(BaseModel):
    items: list[TenantDTO]
    next_cursor: str | None


class TenantWithTaskDTO(BaseModel):
    tenant: TenantDTO
    task: TaskDTO
