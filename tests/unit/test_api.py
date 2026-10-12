from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from cp.db.session import SessionFactory
from cp.domain.enums import TenantStatus
from cp.domain.errors import DomainError
from cp.main import create_app
from cp.services.tenants import create_tenant, delete_tenant


def test_liveness_is_available_without_dependencies() -> None:
    client = TestClient(create_app())

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_validation_errors_use_contract_shape() -> None:
    client = TestClient(create_app())

    response = client.post("/v1/tenants", json={"slug": "Invalid Slug", "name": " "})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["details"]["fields"]


@pytest.mark.asyncio
async def test_delete_tenant_rejects_when_an_open_task_already_exists() -> None:
    slug = f"tenant-{uuid4().hex[:8]}"

    async with SessionFactory() as session:
        tenant, _ = await create_tenant(session, slug, "Open task tenant")
        tenant.status = TenantStatus.ACTIVE
        tenant.version = 2
        tenant.updated_at = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        await session.flush()
        await session.commit()

    with pytest.raises(DomainError, match="open task"):
        async with SessionFactory() as session:
            await delete_tenant(session, tenant.id)
