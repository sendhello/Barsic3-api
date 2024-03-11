import logging
from datetime import date
from typing import Self

from sqlalchemy import Column, String, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import JSONB

from db.postgres import Base, async_session

from .mixins import CRUDMixin, IDMixin


logger = logging.getLogger(__name__)


class ReportCacheModel(Base, IDMixin, CRUDMixin):
    """Таблица с кешом сгенерированных отчетов."""

    __tablename__ = "report_cache"
    report_date = Column(String(255), nullable=False)
    report_type = Column(String(255), nullable=False)
    report_data = Column(JSONB, nullable=False, default=dict)
    __table_args__ = (
        UniqueConstraint("report_date", "report_type", name="unique_report"),
    )

    @classmethod
    async def get_by_date(cls, report_type: str, report_date: date) -> Self:
        async with async_session() as session:
            request = select(cls).where(
                cls.report_date == report_date.isoformat(),
                cls.report_type == report_type,
            )
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity
