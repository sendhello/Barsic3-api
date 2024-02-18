from datetime import timedelta
from logging import config as logging_config

from core.logger import LOGGING
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings


# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


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


class AppSettings(BaseSettings):
    """Настройки приложения."""

    project_name: str = Field("Barsic", validation_alias="PROJECT_NAME")
    debug: bool = Field(False, validation_alias="DEBUG")

    mssql_driver_type: str = Field(validation_alias="MSSQL_DRIVER_TYPE")
    mssql_server: str = Field(validation_alias="MSSQL_SERVER")
    mssql_user: str = Field(validation_alias="MSSQL_USER")
    mssql_pwd: str = Field(validation_alias="MSSQL_PWD")
    mssql_database1: str = Field(validation_alias="MSSQL_DATABASE1")
    mssql_database2: str = Field(validation_alias="MSSQL_DATABASE2")
    mssql_database_bitrix: str = Field(validation_alias="MSSQL_DATABASE_BITRIX")
    mssql_server_rk: str = Field(validation_alias="MSSQL_SERVER_RK")
    mssql_user_rk: str = Field(validation_alias="MSSQL_USER_RK")
    mssql_pwd_rk: str = Field(validation_alias="MSSQL_PWD_RK")
    mssql_database_rk: str = Field(validation_alias="MSSQL_DATABASE_RK")
    reportXML: str = Field(validation_alias="REPORT_XML")
    agentXML: str = Field(validation_alias="AGENT_XML")
    itogreportXML: str = Field(validation_alias="TOTAL_REPORT_XML")
    local_folder: str = Field(validation_alias="LOCAL_FOLDER")
    path: str = Field(validation_alias="PATH")
    credentials_file: str = Field(validation_alias="CREDENTIALS_FILE")
    list_google_docs: str = Field(validation_alias="LIST_GOOGLE_DOCS")
    yadisk_token: str = Field(validation_alias="YADISK_TOKEN")
    telegram_token: str = Field(validation_alias="TELEGRAM_TOKEN")
    telegram_chanel_id: str = Field(validation_alias="TELEGRAM_CHANEL_ID")
    telegram_proxy_use: str = Field(validation_alias="TELEGRAM_PROXY_USE")
    telegram_proxy_type: str = Field(validation_alias="TELEGRAM_PROXY_TYPE")
    telegram_proxy_ip: str = Field(validation_alias="TELEGRAM_PROXY_IP")
    telegram_proxy_port: str = Field(validation_alias="TELEGRAM_PROXY_PORT")
    telegram_proxy_auth: str = Field(validation_alias="TELEGRAM_PROXY_AUTH")
    telegram_proxy_username: str = Field(validation_alias="TELEGRAM_PROXY_USERNAME")
    telegram_proxy_password: str = Field(validation_alias="TELEGRAM_PROXY_PASSWORD")
    google_all_read: str = Field(validation_alias="GOOGLE_ALL_READ")
    google_reader_list: str = Field(validation_alias="GOOGLE_READER_LIST")
    google_writer_list: str = Field(validation_alias="GOOGLE_WRITER_LIST")


class Settings(PostgresSettings, RedisSettings, AppSettings):
    """Все настройки."""
    pass


settings = Settings()
