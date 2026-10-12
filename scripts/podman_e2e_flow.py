import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")
BROKER_MANAGEMENT_BASE = os.getenv("BROKER_MANAGEMENT_BASE", "http://127.0.0.1:15673")
BROKER_USER = os.getenv("BROKER_USER", "admin")
BROKER_PASSWORD = os.getenv("BROKER_PASSWORD", "change-me")
POSTGRES_CONTAINER = "tenant-postgres"


class ScenarioError(RuntimeError):
    pass


def run_podman(*arguments: str, check: bool = True) -> str:
    result = subprocess.run(
        ["podman", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if check and result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ScenarioError(f"podman {' '.join(arguments)} failed: {detail}")
    return result.stdout.strip()


def ensure_compose_provider() -> None:
    try:
        run_podman("compose", "version")
    except ScenarioError as exc:
        shell_hint = (
            "On Windows, run this Python script from PowerShell so it uses the "
            "Windows Podman Compose provider. In WSL, install/configure a Compose "
            "provider for that Podman environment."
        )
        raise ScenarioError(f"Podman Compose is unavailable. {shell_hint}\n{exc}") from exc


class WorkerController:
    def __init__(self) -> None:
        self.temporary_name = f"tenant-e2e-worker-{uuid4().hex[:8]}"
        self.default_worker_stopped = False
        self.temporary_worker_started = False

    def stop_default(self) -> None:
        print("Stopping the default Compose worker...", flush=True)
        run_podman("compose", "stop", "worker")
        self.default_worker_stopped = True
        print("Default worker stopped.", flush=True)

    def start_override(self, options: list[str]) -> None:
        self.stop_default()
        run_podman(
            "compose",
            "run",
            "--detach",
            "--no-deps",
            "--name",
            self.temporary_name,
            "worker",
            "python",
            "-m",
            "function.worker",
            *options,
        )
        self.temporary_worker_started = True
        print("Temporary worker started; waiting for RabbitMQ connection...", flush=True)

        for _ in range(30):
            logs = run_podman("logs", self.temporary_name, check=False)
            if "Worker listening on queue" in logs:
                return
            running = run_podman(
                "inspect",
                "--format",
                "{{.State.Running}}",
                self.temporary_name,
                check=False,
            )
            if running.lower() != "true":
                raise ScenarioError(
                    "Temporary worker exited before it started listening:\n"
                    + run_podman("logs", self.temporary_name, check=False)
                )
            time.sleep(1)
        raise ScenarioError(
            "Timed out waiting for the temporary worker to connect:\n"
            + run_podman("logs", self.temporary_name, check=False)
        )

    def cleanup(self) -> None:
        if self.temporary_worker_started:
            result = subprocess.run(
                ["podman", "rm", "-f", self.temporary_name],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode:
                print(
                    f"Warning: could not remove temporary worker: {result.stderr.strip()}",
                    file=sys.stderr,
                )
        if self.default_worker_stopped:
            try:
                run_podman("compose", "up", "-d", "worker")
            except ScenarioError as exc:
                print(f"Warning: could not restore the default worker: {exc}", file=sys.stderr)


def wait_for_api(client: httpx.Client) -> None:
    last_error = "API did not become ready"
    print(f"Checking API readiness at {API_BASE}/readyz...", flush=True)
    for _ in range(30):
        try:
            response = client.get("/readyz")
            if response.is_success:
                print("API ready; database and broker are connected.", flush=True)
                return
            last_error = f"API readiness returned HTTP {response.status_code}: {response.text}"
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(1)
    raise ScenarioError(f"API is not ready at {API_BASE}: {last_error}")


def create_tenant(client: httpx.Client, scenario: int) -> tuple[str, str, dict[str, str]]:
    slug = f"podman-e2e-{scenario}-{uuid4().hex[:8]}"
    response = client.post(
        "/v1/tenants",
        json={"slug": slug, "name": f"Podman E2E scenario {scenario}"},
    )
    if response.status_code != 201:
        raise ScenarioError(
            f"Tenant creation failed with HTTP {response.status_code}: {response.text}"
        )
    body = response.json()
    tenant_id = body["tenant"]["id"]
    task_id = body["task"]["id"]
    task = body["task"]
    task_event = {
        "id": task["id"],
        "type": task["type"],
        "tenant_id": task["tenant_id"],
        "status": task["status"],
        "created_at": task["created_at"],
    }
    print(f"created tenant_id={tenant_id} task_id={task_id}", flush=True)
    return tenant_id, task_id, task_event


def read_state(client: httpx.Client, tenant_id: str, task_id: str) -> tuple[str, str]:
    tenant_response = client.get(f"/v1/tenants/{tenant_id}")
    tenant_response.raise_for_status()
    task_response = client.get(f"/v1/tasks/{task_id}")
    task_response.raise_for_status()
    return tenant_response.json()["status"], task_response.json()["status"]


def wait_for_state(
    client: httpx.Client,
    tenant_id: str,
    task_id: str,
    expected_tenant: str,
    expected_task: str,
    *,
    timeout_seconds: int = 240,
) -> None:
    previous = ""
    for _ in range(timeout_seconds // 2):
        tenant_status, task_status = read_state(client, tenant_id, task_id)
        current = f"{tenant_status}/{task_status}"
        if current != previous:
            print(f"tenant={tenant_status} task={task_status}", flush=True)
            previous = current
        if tenant_status == expected_tenant and task_status == expected_task:
            return
        time.sleep(2)
    raise ScenarioError(
        f"Timed out waiting for tenant={expected_tenant} task={expected_task}; "
        f"last state was {previous}"
    )


def publish_update(task_id: str, status: str) -> None:
    update = {
        "update_id": str(uuid4()),
        "task_id": task_id,
        "status": status,
        "error": None,
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    request = {
        "properties": {"content_type": "application/json"},
        "routing_key": "task.update",
        "payload": json.dumps(update, separators=(",", ":")),
        "payload_encoding": "string",
    }
    response = httpx.post(
        f"{BROKER_MANAGEMENT_BASE}/api/exchanges/%2F/cp.task-updates/publish",
        auth=(BROKER_USER, BROKER_PASSWORD),
        json=request,
        timeout=10,
    )
    response.raise_for_status()
    if not response.json().get("routed"):
        raise ScenarioError(f"RabbitMQ did not route the {status} update: {response.text}")
    print(f"published {status} update for task_id={task_id}")


def publish_duplicate_task(task_event: dict[str, str]) -> None:
    request = {
        "properties": {
            "content_type": "application/json",
            "delivery_mode": 2,
            "message_id": task_event["id"],
        },
        "routing_key": "task.deploy",
        "payload": json.dumps(task_event, separators=(",", ":")),
        "payload_encoding": "string",
    }
    response = httpx.post(
        f"{BROKER_MANAGEMENT_BASE}/api/exchanges/%2F/cp.tasks/publish",
        auth=(BROKER_USER, BROKER_PASSWORD),
        json=request,
        timeout=10,
    )
    response.raise_for_status()
    if not response.json().get("routed"):
        raise ScenarioError(f"RabbitMQ did not route the duplicate task event: {response.text}")
    print(f"republished task event task_id={task_event['id']}", flush=True)


def processed_update_count(task_id: str) -> int:
    result = run_podman(
        "exec",
        POSTGRES_CONTAINER,
        "psql",
        "-U",
        "admin",
        "-d",
        "tenant_db",
        "-tAc",
        f"SELECT count(*) FROM processed_updates WHERE task_id = '{task_id}';",
    )
    return int(result)


def wait_for_processed_updates(task_id: str, expected: int) -> None:
    for _ in range(30):
        count = processed_update_count(task_id)
        if count >= expected:
            print(f"processed_updates={count}", flush=True)
            return
        time.sleep(1)
    raise ScenarioError(f"Timed out waiting for {expected} processed updates; got {count}")


def wait_for_duplicate_logs(task_id: str, minimum: int = 2) -> None:
    for _ in range(30):
        logs = run_podman("compose", "logs", "--no-color", "api", check=False)
        duplicate_count = sum(
            "outcome=duplicate" in line and f"task_id={task_id}" in line
            for line in logs.splitlines()
        )
        if duplicate_count >= minimum:
            print(f"consumer acknowledged {duplicate_count} duplicate updates", flush=True)
            return
        time.sleep(1)
    raise ScenarioError("Expected duplicate worker updates were not observed in API logs")


def run_scenario(scenario: int, client: httpx.Client, worker: WorkerController) -> None:
    labels = {
        1: "normal successful deployment",
        2: "forced task failure",
        3: "duplicate worker updates",
        4: "terminal update before in_progress",
        5: "duplicate task event delivery",
    }
    print(f"Scenario {scenario}: {labels[scenario]}")

    if scenario in (2, 3):
        ensure_compose_provider()
    if scenario == 2:
        worker.start_override(
            [
                "--fail-rate",
                "1",
                "--min-delay-ms",
                "0",
                "--max-delay-ms",
                "0",
                "--max-attempts",
                "1",
            ]
        )
    elif scenario == 3:
        worker.start_override(
            [
                "--fail-rate",
                "0",
                "--min-delay-ms",
                "0",
                "--max-delay-ms",
                "0",
                "--duplicate-rate",
                "1",
            ]
        )
    elif scenario == 4:
        ensure_compose_provider()
        worker.stop_default()

    tenant_id, task_id, task_event = create_tenant(client, scenario)

    if scenario == 1:
        wait_for_state(client, tenant_id, task_id, "active", "done")
    elif scenario == 2:
        wait_for_state(client, tenant_id, task_id, "failed", "failed")
        print("forced failure scenario passed")
    elif scenario == 3:
        wait_for_state(client, tenant_id, task_id, "active", "done")
        wait_for_processed_updates(task_id, 2)
        wait_for_duplicate_logs(task_id)
        print("duplicate delivery was idempotent")
    elif scenario == 4:
        publish_update(task_id, "done")
        wait_for_state(client, tenant_id, task_id, "active", "done")
        publish_update(task_id, "in_progress")
        wait_for_processed_updates(task_id, 3)
        tenant_status, task_status = read_state(client, tenant_id, task_id)
        if tenant_status != "active" or task_status != "done":
            raise ScenarioError(
                f"Late in_progress update regressed state: "
                f"tenant={tenant_status} task={task_status}"
            )
        print(f"out-of-order update was handled; tenant={tenant_status} task={task_status}")
    else:
        wait_for_state(client, tenant_id, task_id, "active", "done")
        wait_for_processed_updates(task_id, 2)
        publish_duplicate_task(task_event)
        wait_for_duplicate_logs(task_id, minimum=1)
        count = processed_update_count(task_id)
        tenant_status, task_status = read_state(client, tenant_id, task_id)
        if count != 2 or tenant_status != "active" or task_status != "done":
            raise ScenarioError(
                "Duplicate task event changed state unexpectedly: "
                f"processed_updates={count} tenant={tenant_status} task={task_status}"
            )
        print("duplicate task event was acknowledged without reapplying its update")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a numbered tenant-control-plane E2E scenario")
    parser.add_argument(
        "scenario",
        nargs="?",
        type=int,
        choices=(1, 2, 3, 4, 5),
        default=1,
        help=(
            "1=success, 2=failure, 3=duplicate updates, "
            "4=out-of-order updates, 5=duplicate task event"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    worker = WorkerController()
    try:
        with httpx.Client(base_url=API_BASE, timeout=10) as client:
            wait_for_api(client)
            run_scenario(args.scenario, client, worker)
        return 0
    except (ScenarioError, httpx.HTTPError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        worker.cleanup()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted; restoring the default worker if it was stopped.", file=sys.stderr)
        raise SystemExit(130)
