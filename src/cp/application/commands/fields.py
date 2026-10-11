from typing import Annotated

from pydantic import Field

Slug = Annotated[str, Field(min_length=3, max_length=28, pattern=r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$")]
TenantName = Annotated[str, Field(min_length=1, max_length=255, pattern=r".*\S.*")]
