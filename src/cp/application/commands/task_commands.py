from uuid import UUID

from pydantic import BaseModel, Field

from cp.domain.enums import TaskStatus, TaskType


class ListTasksCommand(BaseModel):
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = None
    tenant_id: UUID | None = None
    status: TaskStatus | None = None
    task_type: TaskType | None = None


class ListTenantTasksCommand(BaseModel):
    tenant_id: UUID
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = None


class GetTaskCommand(BaseModel):
    task_id: UUID
