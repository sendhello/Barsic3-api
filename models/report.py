from typing import Self

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import joinedload, relationship

from db.postgres import Base, async_session

from .mixins import CRUDMixin, IDMixin, TitleMixin


class ReportNameModel(Base, IDMixin, TitleMixin, CRUDMixin):
    """Название отчета."""

    __tablename__ = "report_name"

    groups = relationship(
        "ReportGroupModel", back_populates="report", passive_deletes=True
    )

    @classmethod
    async def get_by_id(cls, id_: UUID) -> Self:
        async with async_session() as session:
            request = select(cls).options(joinedload(cls.groups)).where(cls.id == id_)
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity

    @classmethod
    async def get_by_title(cls, title: str) -> Self:
        async with async_session() as session:
            request = (
                select(cls).options(joinedload(cls.groups)).where(cls.title == title)
            )
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity


class ReportGroupModel(Base, IDMixin, CRUDMixin):
    """Группа элементов в отчете"""

    __tablename__ = "report_group"

    title = Column(String(255), nullable=False)
    parent_id = Column(UUID)
    report_name_id = Column(
        UUID, ForeignKey("report_name.id", ondelete="CASCADE"), nullable=False
    )
    report = relationship("ReportNameModel", back_populates="groups")
    elements = relationship(
        "ReportElementModel", back_populates="group", passive_deletes=True
    )
    __table_args__ = (
        UniqueConstraint("title", "report_name_id", name="unique_title_groups"),
    )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.title}>"

    @classmethod
    async def get_by_id(cls, id_: UUID) -> Self:
        async with async_session() as session:
            request = select(cls).options(joinedload(cls.elements)).where(cls.id == id_)
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity

    @classmethod
    async def get_by_title(cls, title: str, report_name_id: UUID) -> Self:
        async with async_session() as session:
            request = (
                select(cls)
                .options(joinedload(cls.elements))
                .where(cls.title == title, cls.report_name_id == report_name_id)
            )
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity

    @classmethod
    async def get_by_report_name_id(cls, report_name_id: UUID) -> list[Self]:
        async with async_session() as session:
            request = select(cls).where(cls.report_name_id == report_name_id)
            result = await session.execute(request)
            entities = result.scalars().all()

        return entities


class ReportElementModel(Base, IDMixin, TitleMixin, CRUDMixin):
    """Элементы отчета."""

    __tablename__ = "report_element"

    title = Column(String(255), nullable=False)
    group_id = Column(
        UUID, ForeignKey("report_group.id", ondelete="CASCADE"), nullable=False
    )
    group = relationship("ReportGroupModel", back_populates="elements")
    __table_args__ = (
        UniqueConstraint("title", "group_id", name="unique_title_elements"),
    )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.title}>"

    @classmethod
    async def get_by_group_id(cls, report_group_id: UUID) -> list[Self]:
        async with async_session() as session:
            request = select(cls).where(cls.group_id == report_group_id)
            result = await session.execute(request)
            entities = result.scalars().all()

        return entities


class GoogleReportIdModel(Base, IDMixin, CRUDMixin):
    """Список Google-документов."""

    __tablename__ = "google_report_ids"
    month = Column(String(255), nullable=False, unique=True)
    doc_id = Column(String(255), nullable=False, unique=True)
    version = Column(Integer, nullable=False)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.title}>"

    @classmethod
    async def get_by_month(cls, month: str) -> Self:
        async with async_session() as session:
            request = select(cls).where(cls.month == month)
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity
