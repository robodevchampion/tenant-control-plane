from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import DateTime, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from cp.db.base import Base


class ProcessedUpdate(Base):
    __tablename__ = "processed_updates"

    update_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    task_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
