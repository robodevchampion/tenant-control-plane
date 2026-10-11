import logging
import time

from fastapi import Request
from starlette.responses import Response

logger = logging.getLogger("cp.http")


async def request_logging(request: Request, call_next) -> Response:
    started_at = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "%s %s -> %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response
