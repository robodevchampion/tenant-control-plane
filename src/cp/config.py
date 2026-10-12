from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_name: str
    db_user: str
    db_password: str
    db_port: int
    db_host: str
    broker_host: str
    broker_port: int
    broker_user: str
    broker_password: str
    outbox_poll_interval_ms: int
    outbox_batch_size: int

    @property
    def database_url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        )

    @property
    def broker_url(self) -> str:
        username = quote(self.broker_user, safe="")
        password = quote(self.broker_password, safe="")
        return f"amqp://{username}:{password}@{self.broker_host}:{self.broker_port}/"


@lru_cache
def get_settings() -> Settings:
    return Settings()
