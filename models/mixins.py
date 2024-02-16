import uuid
from datetime import datetime
from typing import Self

from db.postgres import async_session
from sqlalchemy import Column, DateTime, select
from sqlalchemy.dialects.postgresql import UUID


class CRUDMixin:
    """Mixin-класс предоставляющий частые CRUD операции над моделями."""

    @classmethod
    async def create(cls, commit=True, **kwargs) -> Self:
        instance = cls(**kwargs)
        return await instance.save(commit=commit)

    async def update(self, commit=True, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

        return await self.save(commit=commit)

    async def delete(self, commit=True):
        async with async_session() as session:
            await session.delete(self)
            if commit:
                await session.commit()

        return self

    async def save(self, commit=True):
        async with async_session() as session:
            session.add(self)
            if commit:
                await session.commit()
                await session.refresh(self)

        return self

    @classmethod
    async def get_all(cls, page: int = 1, page_size: int = 20) -> list[Self]:
        async with async_session() as session:
            request = select(cls).limit(page_size).offset((page - 1) * page_size)
            result = await session.execute(request)
            entities = result.scalars().all()

        return entities


class IDMixin:
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        unique=True,
        nullable=False,
    )
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow())

    @classmethod
    async def get_by_id(cls, id_: UUID) -> Self:
        async with async_session() as session:
            request = select(cls).where(cls.id == id_)
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity
