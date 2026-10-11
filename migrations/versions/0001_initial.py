"""Create tenant, task, and messaging persistence tables."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

tenant_status = postgresql.ENUM(
    "provisioning",
    "active",
    "updating",
    "destroying",
    "destroyed",
    "failed",
    name="tenant_status",
    create_type=False,
)
task_type = postgresql.ENUM("deploy", "update", "destroy", name="task_type", create_type=False)
task_status = postgresql.ENUM(
    "accepted", "in_progress", "done", "failed", name="task_status", create_type=False
)


def upgrade() -> None:
    bind = op.get_bind()
    tenant_status.create(bind, checkfirst=True)
    task_type.create(bind, checkfirst=True)
    task_status.create(bind, checkfirst=True)

    op.create_table(
        "tenants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=28), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", tenant_status, nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("length(btrim(name)) > 0", name="tenants_name_nonempty"),
        sa.CheckConstraint("slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'", name="tenants_slug_format"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="tenants_slug_key"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("type", task_type, nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("status", task_status, nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "tasks_tenant_created_idx",
        "tasks",
        ["tenant_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index("tasks_created_idx", "tasks", [sa.text("created_at DESC"), sa.text("id DESC")])
    op.create_index(
        "tasks_one_open_per_tenant",
        "tasks",
        ["tenant_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('accepted', 'in_progress')"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("routing_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "outbox_unpublished_idx",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_updates",
        sa.Column("update_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("update_id"),
    )


def downgrade() -> None:
    op.drop_table("processed_updates")
    op.drop_index("outbox_unpublished_idx", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("tasks_one_open_per_tenant", table_name="tasks")
    op.drop_index("tasks_created_idx", table_name="tasks")
    op.drop_index("tasks_tenant_created_idx", table_name="tasks")
    op.drop_table("tasks")
    op.drop_table("tenants")
    task_status.drop(op.get_bind(), checkfirst=True)
    task_type.drop(op.get_bind(), checkfirst=True)
    tenant_status.drop(op.get_bind(), checkfirst=True)
