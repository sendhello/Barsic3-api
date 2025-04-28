from logging import config as logging_config

from pydantic import AnyUrl, EmailStr, Field
from pydantic_settings import BaseSettings

from core.logger import LOGGING


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
    show_traceback: bool = Field(False, validation_alias="SHOW_TRACEBACK")

    mssql_driver_type: str = Field(validation_alias="MSSQL_DRIVER_TYPE")
    mssql_server: str = Field(validation_alias="MSSQL_SERVER")
    mssql_user: str = Field(validation_alias="MSSQL_USER")
    mssql_pwd: str = Field(validation_alias="MSSQL_PWD")
    mssql_database1: str = Field(validation_alias="MSSQL_DATABASE1")
    mssql_database2: str = Field(validation_alias="MSSQL_DATABASE2")
    mssql_server_rk: str = Field(validation_alias="MSSQL_SERVER_RK")
    mssql_user_rk: str = Field(validation_alias="MSSQL_USER_RK")
    mssql_pwd_rk: str = Field(validation_alias="MSSQL_PWD_RK")
    mssql_database_rk: str = Field(validation_alias="MSSQL_DATABASE_RK")
    local_folder: str = Field(validation_alias="LOCAL_FOLDER")
    report_path: str = Field(validation_alias="REPORT_PATH")
    yadisk_token: str = Field(validation_alias="YADISK_TOKEN")
    telegram_token: str = Field(validation_alias="TELEGRAM_TOKEN")
    telegram_chanel_id: str = Field(validation_alias="TELEGRAM_CHANEL_ID")

    report_names: str = Field(validation_alias="REPORT_NAMES")


class GoogleApiSettings(BaseSettings):
    """Настройки google API."""

    google_all_read: str = Field(validation_alias="GOOGLE_ALL_READ")
    google_reader_list: str = Field(validation_alias="GOOGLE_READER_LIST")
    google_writer_list: str = Field(validation_alias="GOOGLE_WRITER_LIST")
    # Настройки Google Service Account
    project_id: str = Field(validation_alias="GOOGLE_API_PROJECT_ID")
    private_key_id: str = Field(validation_alias="GOOGLE_API_PRIVATE_KEY_ID")
    private_key: str = Field(validation_alias="GOOGLE_API_PRIVATE_KEY")
    client_email: EmailStr = Field(validation_alias="GOOGLE_API_CLIENT_EMAIL")
    client_id: str = Field(validation_alias="GOOGLE_API_CLIENT_ID")
    client_x509_cert_url: AnyUrl = Field(
        validation_alias="GOOGLE_API_CLIENT_X509_CERT_URL"
    )

    @property
    def google_service_account_config(self):
        return {
            "type": "service_account",
            "project_id": self.project_id,
            "private_key_id": self.private_key_id,
            "private_key": self.private_key,
            "client_email": self.client_email,
            "client_id": self.client_id,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": self.client_x509_cert_url,
        }


class Settings(PostgresSettings, RedisSettings, AppSettings):
    """Все настройки."""

    google_api_settings: GoogleApiSettings = GoogleApiSettings()


settings = Settings()
