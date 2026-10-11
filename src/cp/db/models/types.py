from enum import StrEnum

from sqlalchemy import Enum as SqlEnum


def enum_type(enum_class: type[StrEnum], name: str) -> SqlEnum:
    return SqlEnum(
        enum_class,
        name=name,
        values_callable=lambda values: [value.value for value in values],
    )
