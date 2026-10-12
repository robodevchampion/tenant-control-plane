import logging
import sys

APP_LOGGERS = (
    "function.publisher",
    "function.consumer",
    "function.worker",
    "function.worker.simulator",
    "cp.services.task_updates",
)


def configure_application_logging() -> None:
    for logger_name in APP_LOGGERS:
        app_logger = logging.getLogger(logger_name)
        app_logger.setLevel(logging.INFO)
        app_logger.propagate = False
        if any(
            getattr(handler, "_tenant_control_plane_handler", False)
            for handler in app_logger.handlers
        ):
            continue

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        setattr(handler, "_tenant_control_plane_handler", True)
        app_logger.addHandler(handler)
