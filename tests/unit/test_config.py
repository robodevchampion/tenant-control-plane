from cp.config import Settings


def test_settings_load_required_values_from_env_file(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DB_NAME=test_db",
                "DB_USER=test_user",
                "DB_PASSWORD=test_password",
                "DB_PORT=5544",
                "DB_HOST=db.example.test",
                "BROKER_HOST=broker.example.test",
                "BROKER_PORT=5673",
                "BROKER_USER=broker_user",
                "BROKER_PASSWORD=broker_password",
                "OUTBOX_POLL_INTERVAL_MS=250",
                "OUTBOX_BATCH_SIZE=12",
                "CONSUMER_PREFETCH=4",
                "CONSUMER_MAX_ATTEMPTS=3",
                "WORKER_FAIL_RATE=0.25",
                "WORKER_MIN_DELAY_MS=10",
                "WORKER_MAX_DELAY_MS=20",
                "WORKER_MAX_ATTEMPTS=2",
                "WORKER_RETRY_BACKOFF_MS=15",
                "WORKER_CONCURRENCY=3",
                "WORKER_DUPLICATE_RATE=0.5",
                "WORKER_SEED=17",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.db_name == "test_db"
    assert settings.db_port == 5544
    assert settings.broker_host == "broker.example.test"
    assert settings.outbox_poll_interval_ms == 250
    assert settings.outbox_batch_size == 12
    assert settings.consumer_prefetch == 4
    assert settings.consumer_max_attempts == 3
    assert settings.worker_fail_rate == 0.25
    assert settings.worker_min_delay_ms == 10
    assert settings.worker_max_delay_ms == 20
    assert settings.worker_max_attempts == 2
    assert settings.worker_retry_backoff_ms == 15
    assert settings.worker_concurrency == 3
    assert settings.worker_duplicate_rate == 0.5
    assert settings.worker_seed == 17