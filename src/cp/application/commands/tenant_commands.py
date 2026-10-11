from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from cp.application.commands.fields import Slug, TenantName


class CreateTenantCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: Slug
    name: TenantName


class ListTenantsCommand(BaseModel):
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = None
    status: str | None = None
    slug: Slug | None = None


class GetTenantCommand(BaseModel):
    tenant_id: UUID


class GetTenantBySlugCommand(BaseModel):
    slug: Slug


class UpdateTenantBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    name: TenantName


class UpdateTenantCommand(UpdateTenantBody):
    tenant_id: UUID


class DeleteTenantCommand(BaseModel):
    tenant_id: UUID
