from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from cp.db.base import Base
from cp.db.models.types import enum_type
from cp.domain.enums import TenantStatus


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        CheckConstraint("length(btrim(name)) > 0", name="tenants_name_nonempty"),
        CheckConstraint(
            "slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'",
            name="tenants_slug_format",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    slug: Mapped[str] = mapped_column(String(28), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[TenantStatus] = mapped_column(
        enum_type(TenantStatus, "tenant_status"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
