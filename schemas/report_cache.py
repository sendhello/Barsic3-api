from datetime import date

from pydantic import Field

from .base import Model
from .mixins import IdMixin


class ReportCacheCreate(Model):
    """Кеш отчета."""

    report_date: date = Field(description="Дата отчета")
    report_type: str = Field(description="Тип отчета")
    report_data: dict = Field(description="Содержимое отчета")


class ReportCache(IdMixin, ReportCacheCreate):
    """Кеш отчета."""

    pass
