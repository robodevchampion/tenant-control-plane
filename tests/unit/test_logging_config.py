import logging

from cp.logging_config import configure_application_logging


def test_application_info_logs_are_enabled(caplog) -> None:
    configure_application_logging()
    logger = logging.getLogger("function.publisher.outbox_publisher")
    caplog.set_level(logging.INFO, logger=logger.name)

    logger.info("publisher logging check")

    assert "publisher logging check" in caplog.text
