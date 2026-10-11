from fastapi import APIRouter
from fastapi.responses import JSONResponse

from cp.application.commands.health_commands import LivenessCommand, ReadinessCommand
from cp.application.core.mediator import Mediator

router = APIRouter(tags=["Health"])


@router.get("/healthz")
async def liveness() -> dict[str, str]:
    return await Mediator().send(LivenessCommand())


@router.get("/readyz")
async def readiness() -> JSONResponse:
    result = await Mediator().send(ReadinessCommand())
    response_status = int(result.pop("http_status"))
    return JSONResponse(status_code=response_status, content=result)
