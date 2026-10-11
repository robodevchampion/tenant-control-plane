from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Text, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from cp.db.base import Base


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index("outbox_unpublished_idx", "id", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tasks.id"), nullable=False
    )
    routing_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
