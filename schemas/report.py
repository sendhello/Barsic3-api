from uuid import UUID

from pydantic import Field

from .base import Model
from .mixins import IdMixin


class ReportElementCreate(Model):
    """Элемент отчета для создания."""

    title: str = Field(description="Название элемента")
    group_id: UUID = Field(description="ID группы")


class ReportElementUpdate(Model):
    """Элемент отчета для редактирования."""

    title: str = Field(description="Название элемента")


class ReportElement(IdMixin, ReportElementCreate):
    """Элемент отчета."""

    pass


class ReportElementDetail(IdMixin, Model):
    """Элемент отчета."""

    title: str = Field(description="Название элемента")


class ReportGroupCreate(Model):
    """Группа элементов в отчете для создания"""

    title: str = Field(description="Название группы")
    parent_id: UUID | None = Field(description="ID родительской группы")
    report_name_id: UUID = Field(description="ID отчета")


class ReportGroupUpdate(Model):
    """Группа элементов в отчете для редактирования."""

    title: str = Field(description="Название группы")


class ReportGroup(IdMixin, ReportGroupCreate):
    """Группа элементов в отчете"""

    pass


class ReportGroupDetail(ReportGroup):
    """Группа элементов с элементами"""

    title: str = Field(description="Название группы")
    parent_id: UUID | None = Field(description="ID родительской группы")
    elements: list[ReportElementDetail] = Field(description="")


class ReportNameCreate(Model):
    """Тип отчета для создания."""

    title: str = Field(description="Название отчета")


class ReportNameUpdate(Model):
    """Тип отчета для обновления."""

    title: str = Field(description="Название отчета")


class ReportName(IdMixin, ReportNameCreate):
    """Тип отчета."""

    pass


class ReportNameDetail(ReportName):
    """Тип отчета со списком отчетов."""

    groups: list[ReportGroup] = Field(description="Группы отчета")


class ReportNameFullDetail(ReportName):
    """Тип отчета со списком отчетов."""

    groups: list[ReportGroupDetail] = Field(description="Группы отчета")
