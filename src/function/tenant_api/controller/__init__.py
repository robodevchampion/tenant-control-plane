from function.tenant_api.controller.health_controller import router as health_router
from function.tenant_api.controller.task_controller import router as task_router
from function.tenant_api.controller.tenant_controller import router as tenant_router

__all__ = ["health_router", "task_router", "tenant_router"]
