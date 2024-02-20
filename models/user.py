from typing import Self

from sqlalchemy import Column, ForeignKey, String, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import joinedload, relationship

from db.postgres import Base, async_session

from .mixins import CRUDMixin, IDMixin


class User(Base, IDMixin, CRUDMixin):
    __tablename__ = "users"

    login = Column(String(255), unique=True)
    email = Column(String(255), unique=True, nullable=False)

    def __init__(
        self,
        email: str,
    ) -> None:
        self.email = email

    @classmethod
    async def get_by_login(cls, username: str) -> Self:
        async with async_session() as session:
            request = (
                select(cls).options(joinedload(cls.role)).where(cls.login == username)
            )
            result = await session.execute(request)
            user = result.scalars().first()

        return user

    @classmethod
    async def get_by_email(cls, email: str) -> Self:
        async with async_session() as session:
            request = (
                select(cls).options(joinedload(cls.role)).where(cls.email == email)
            )
            result = await session.execute(request)
            user = result.scalars().first()

        return user

    @classmethod
    async def get_all(cls, page: int = 1, page_size: int = 20) -> list[Self]:
        async with async_session() as session:
            request = (
                select(cls)
                .options(joinedload(cls.role))
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
            result = await session.execute(request)
            users = result.scalars().all()

        return users

    @classmethod
    async def get_by_id(cls, id_: UUID) -> Self:
        async with async_session() as session:
            request = select(cls).options(joinedload(cls.role)).where(cls.id == id_)
            result = await session.execute(request)
            users = result.scalars().first()

        return users

    def __repr__(self) -> str:
        return f"<User {self.email}>"


class Social(Base, IDMixin, CRUDMixin):
    __tablename__ = "socials"

    social_id = Column(String(255), nullable=False, unique=True)
    type = Column(String(255), nullable=False)
    user_id = Column(UUID, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    user = relationship("User", back_populates="socials")

    @classmethod
    async def get_by_social_id(cls, social_id: str) -> Self:
        async with async_session() as session:
            request = select(cls).where(cls.social_id == social_id)
            result = await session.execute(request)
            entity = result.scalars().first()

        return entity
