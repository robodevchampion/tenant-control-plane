#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
SLUG="${SLUG:-podman-e2e-$(date +%s)}"
NAME="${NAME:-Podman E2E flow}"

curl -fsS "$API_BASE/healthz" >/dev/null

payload=$(python - <<PY
import json
print(json.dumps({"slug": "${SLUG}", "name": "${NAME}"}))
PY
)

response=$(curl -fsS -X POST "$API_BASE/v1/tenants" -H 'Content-Type: application/json' --data "$payload")

tenant_id=$(python -c 'import json, sys; print(json.load(sys.stdin)["tenant"]["id"])' <<<"$response")

task_id=$(python -c 'import json, sys; print(json.load(sys.stdin)["task"]["id"])' <<<"$response")

echo "created tenant_id=$tenant_id task_id=$task_id"

last_status=""
for _ in $(seq 1 120); do
  tenant_json=$(curl -fsS "$API_BASE/v1/tenants/$tenant_id")
  tenant_status=$(python -c 'import json, sys; print(json.load(sys.stdin)["status"])' <<<"$tenant_json")
  task_json=$(curl -fsS "$API_BASE/v1/tasks/$task_id")
  task_status=$(python -c 'import json, sys; print(json.load(sys.stdin)["status"])' <<<"$task_json")
  current_status="$tenant_status/$task_status"

  if [[ "$current_status" != "$last_status" ]]; then
    echo "tenant=$tenant_status task=$task_status"
    last_status="$current_status"
  fi

  if [[ "$tenant_status" == "active" && "$task_status" == "done" ]]; then
    exit 0
  fi

  if [[ "$tenant_status" == "failed" || "$task_status" == "failed" ]]; then
    task_error=$(python -c 'import json, sys; print(json.load(sys.stdin).get("error") or "no error detail")' <<<"$task_json")
    echo "provisioning failed: $task_error" >&2
    exit 1
  fi

  sleep 2
done

echo "timed out waiting for tenant to become active and task to be done" >&2
exit 1
