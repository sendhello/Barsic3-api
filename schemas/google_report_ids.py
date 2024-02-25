from pydantic import Field

from .base import Model
from .mixins import IdMixin


class GoogleReportIdCreate(Model):
    month: str = Field(description="Месяц отчета")
    doc_id: str = Field(description="ID документа Google Report")
    version: int = Field(description="Версия документа")


class GoogleReportIdUpdate(Model):
    month: str = Field(description="Месяц отчета")


class GoogleReportId(IdMixin, GoogleReportIdCreate):
    pass
