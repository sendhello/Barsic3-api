from enum import StrEnum
from pydantic import BaseModel


class DBName(StrEnum):
    """Название базы данных."""

    AQUA = "aqua"
    BEACH = "beach"


class Company(BaseModel):
    """Информация об организации."""

    id: int
    name: str
    db_name: DBName
