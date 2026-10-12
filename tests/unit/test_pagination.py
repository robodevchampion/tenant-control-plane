from datetime import UTC, datetime
from uuid import uuid4

import pytest

from cp.application.core.pagination import decode_cursor, encode_cursor
from cp.domain.errors import DomainError


def test_cursor_round_trips_timestamp_and_id() -> None:
    created_at = datetime(2026, 10, 11, 10, 15, 30, 123456, tzinfo=UTC)
    item_id = uuid4()

    assert decode_cursor(encode_cursor(created_at, item_id)) == (created_at, item_id)


def test_malformed_cursor_is_a_validation_error() -> None:
    with pytest.raises(DomainError) as error:
        decode_cursor("not-a-cursor")

    assert error.value.code == "validation_error"
