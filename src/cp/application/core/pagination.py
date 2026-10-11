import base64
import binascii
import json
from datetime import datetime
from uuid import UUID

from cp.domain.errors import DomainError


def encode_cursor(created_at: datetime, item_id: UUID) -> str:
    payload = json.dumps({"created_at": created_at.isoformat(), "id": str(item_id)}).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.b64decode(cursor + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw)
        created_at = datetime.fromisoformat(payload["created_at"])
        if created_at.tzinfo is None:
            raise ValueError("Cursor timestamp must include a timezone")
        return created_at, UUID(payload["id"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error) as exc:
        raise DomainError(
            "validation_error",
            "Request validation failed.",
            422,
            {"fields": [{"field": "cursor", "message": "Cursor is invalid."}]},
        ) from exc
