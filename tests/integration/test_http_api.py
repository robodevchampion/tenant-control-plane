import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, select, update

from cp.db.models import Outbox, Task, Tenant
from cp.db.session import SessionFactory, engine
from cp.domain.enums import TaskStatus, TenantStatus


@pytest.mark.asyncio
async def test_documented_api_operations_over_http() -> None:
    base_url = os.getenv("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    tenant_ids: list[str] = []

    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        try:
            health = await client.get("/healthz")
        except httpx.ConnectError:
            pytest.skip(f"API server is not reachable at {base_url}")

        assert health.status_code == 200
        assert health.json() == {"status": "ok"}

        try:
            readiness = await client.get("/readyz")
            assert readiness.status_code in {200, 503}
            assert set(readiness.json()["checks"]) == {"database", "broker"}

            invalid_create = await client.post(
                "/v1/tenants", json={"slug": "Not Valid", "name": ""}
            )
            assert invalid_create.status_code == 422
            assert invalid_create.json()["error"]["code"] == "validation_error"

            created = []
            for suffix in range(2):
                slug = f"http-test-{uuid4().hex[:12]}"
                response = await client.post(
                    "/v1/tenants",
                    json={"slug": slug, "name": f"HTTP test tenant {suffix}"},
                )
                assert response.status_code == 201, response.text
                tenant = response.json()["tenant"]
                task = response.json()["task"]
                tenant_ids.append(tenant["id"])
                created.append((tenant, task))
                assert response.headers["location"] == f"/v1/tenants/{tenant['id']}"
                assert tenant["status"] == "provisioning"
                assert task["type"] == "deploy" and task["status"] == "accepted"

            duplicate = await client.post(
                "/v1/tenants",
                json={"slug": created[0][0]["slug"], "name": "Duplicate"},
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["error"]["code"] == "tenant_already_exists"

            tenant, deploy_task = created[0]
            assert (await client.get(f"/v1/tenants/{tenant['id']}")).status_code == 200
            assert (await client.get(f"/v1/tenants/by-slug/{tenant['slug']}")).status_code == 200
            assert (await client.get("/v1/tenants", params={"slug": tenant["slug"]})).json()[
                "items"
            ][0]["id"] == tenant["id"]

            tenant_page = await client.get(
                "/v1/tenants", params={"status": "provisioning", "limit": 1}
            )
            assert tenant_page.status_code == 200
            assert tenant_page.json()["next_cursor"]
            next_tenants = await client.get(
                "/v1/tenants",
                params={
                    "status": "provisioning",
                    "limit": 1,
                    "cursor": tenant_page.json()["next_cursor"],
                },
            )
            assert next_tenants.status_code == 200

            task_page = await client.get("/v1/tasks", params={"status": "accepted", "limit": 1})
            assert task_page.status_code == 200
            assert task_page.json()["next_cursor"]
            assert (await client.get(f"/v1/tasks/{deploy_task['id']}")).status_code == 200
            assert (
                await client.get("/v1/tasks", params={"tenant_id": tenant["id"], "type": "deploy"})
            ).status_code == 200

            nested_tasks = await client.get(f"/v1/tenants/{tenant['id']}/tasks")
            assert nested_tasks.status_code == 200
            assert nested_tasks.json()["items"][0]["id"] == deploy_task["id"]
            unknown_tenant = str(uuid4())
            missing_nested = await client.get(f"/v1/tenants/{unknown_tenant}/tasks")
            assert missing_nested.status_code == 404
            assert missing_nested.json()["error"]["code"] == "tenant_not_found"

            assert (await client.get(f"/v1/tenants/{uuid4()}")).json()["error"][
                "code"
            ] == "tenant_not_found"
            assert (await client.get(f"/v1/tasks/{uuid4()}")).json()["error"][
                "code"
            ] == "task_not_found"
            assert (await client.get("/v1/tenants?limit=0")).status_code == 422
            assert (
                await client.get(f"/v1/tenants/{tenant['id']}/tasks?cursor=invalid")
            ).status_code == 422

            blocked_patch = await client.patch(
                f"/v1/tenants/{tenant['id']}",
                json={"version": tenant["version"], "name": "Blocked"},
            )
            assert blocked_patch.status_code == 409
            assert blocked_patch.json()["error"]["code"] == "tenant_update_not_allowed"

            async with SessionFactory() as session:
                async with session.begin():
                    await session.execute(
                        update(Tenant)
                        .where(Tenant.id.in_(tenant_ids))
                        .values(status=TenantStatus.ACTIVE)
                    )
                    await session.execute(
                        update(Task)
                        .where(Task.tenant_id.in_(tenant_ids))
                        .values(status=TaskStatus.DONE)
                    )

            patch = await client.patch(
                f"/v1/tenants/{tenant['id']}",
                json={"version": tenant["version"], "name": "Updated over HTTP"},
            )
            assert patch.status_code == 202, patch.text
            assert patch.headers["location"] == f"/v1/tasks/{patch.json()['task']['id']}"
            assert patch.json()["tenant"]["status"] == "updating"
            assert patch.json()["task"]["type"] == "update"

            stale_patch = await client.patch(
                f"/v1/tenants/{tenant['id']}",
                json={"version": tenant["version"], "name": "Stale version"},
            )
            assert stale_patch.status_code == 409
            assert stale_patch.json()["error"]["code"] == "tenant_version_conflict"

            delete_tenant, _ = created[1]
            delete_response = await client.delete(f"/v1/tenants/{delete_tenant['id']}")
            assert delete_response.status_code == 202, delete_response.text
            assert delete_response.json()["tenant"]["status"] == "destroying"
            assert delete_response.json()["task"]["type"] == "destroy"
        finally:
            if tenant_ids:
                async with SessionFactory() as session:
                    async with session.begin():
                        task_ids = select(Task.id).where(Task.tenant_id.in_(tenant_ids))
                        await session.execute(delete(Outbox).where(Outbox.task_id.in_(task_ids)))
                        await session.execute(delete(Task).where(Task.tenant_id.in_(tenant_ids)))
                        await session.execute(delete(Tenant).where(Tenant.id.in_(tenant_ids)))
    await engine.dispose()
