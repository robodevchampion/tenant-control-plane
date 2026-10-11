from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, Text, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from cp.db.base import Base
from cp.db.models.types import enum_type
from cp.domain.enums import TaskStatus, TaskType


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("tasks_tenant_created_idx", "tenant_id", text("created_at DESC"), text("id DESC")),
        Index("tasks_created_idx", text("created_at DESC"), text("id DESC")),
        Index(
            "tasks_one_open_per_tenant",
            "tenant_id",
            unique=True,
            postgresql_where=text("status IN ('accepted', 'in_progress')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    type: Mapped[TaskType] = mapped_column(enum_type(TaskType, "task_type"), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    status: Mapped[TaskStatus] = mapped_column(enum_type(TaskStatus, "task_status"), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
