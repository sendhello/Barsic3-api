from datetime import timedelta
from logging import config as logging_config

from core.logger import LOGGING
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings


# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


class ProjectSettings(BaseSettings):
    """Основные настройки."""

    project_name: str = Field("Barsic", validation_alias="PROJECT_NAME")
    debug: bool = Field(False, validation_alias="DEBUG")


class PostgresSettings(BaseSettings):
    """Настройки Postgres."""

    echo_database: bool = Field(False, validation_alias="ECHO_DATABASE")
    postgres_host: str = Field(validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(validation_alias="POSTGRES_DB")
    postgres_user: str = Field(validation_alias="POSTGRES_USER")
    postgres_password: str = Field(validation_alias="POSTGRES_PASSWORD")

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}"
            f":{self.postgres_password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_db}"
        )


class RedisSettings(BaseSettings):
    """Настройки Redis."""

    redis_host: str = Field("127.0.0.1", validation_alias="REDIS_HOST")
    redis_port: int = Field(6379, validation_alias="REDIS_PORT")


class Settings(ProjectSettings, PostgresSettings, RedisSettings):
    """Все настройки."""
    pass


settings = Settings()
