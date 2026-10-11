from datetime import UTC, datetime


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(UTC).replace(microsecond=(value.microsecond // 1000) * 1000)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
