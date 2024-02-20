from pydantic import EmailStr

from .base import Model
from .mixins import IdMixin


class BaseUser(Model):
    email: EmailStr


class UserCreated(BaseUser, IdMixin):
    """Модель пользователя при выводе после регистрации."""

    pass
