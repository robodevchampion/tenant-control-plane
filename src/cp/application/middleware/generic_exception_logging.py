import logging

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("cp.errors")


async def exception_callback(_request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled API exception", exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "An unexpected error occurred.",
                "details": {},
            }
        },
    )
