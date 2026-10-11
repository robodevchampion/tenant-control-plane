from functools import lru_cache
from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    db_name: str = "tenant_db"
    db_user: str = "admin"
    db_password: str = "admin"
    db_port: int = 5432
    db_host: str = "localhost"
    broker_host: str = "localhost"
    broker_port: int = 5672
    broker_user: str = "guest"
    broker_password: str = "guest"
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 50

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
