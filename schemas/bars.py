from datetime import datetime
from uuid import UUID

from pydantic import Field

from .base import Model


class Category(Model):
    """Категории объектов барса. Среди прочих тут хранятся названия тарифов."""

    category_id: int = Field(alias="CategoryId")
    stock_type: int = Field(alias="StockType")
    name: str = Field(alias="Name")
    organization_id: int = Field(alias="OrganizationId")
    guid: UUID = Field(alias="Guid")
    change_time: datetime = Field(alias="ChangeTime")


class Organisation(Model):
    """Организации."""

    super_account_id: int = Field(alias="SuperAccountId")
    type: int = Field(alias="Type")
    descr: str = Field(alias="Descr")
    is_stuff: bool = Field(alias="IsStuff")
    is_blocked: bool = Field(alias="IsBlocked")
    address: str = Field(alias="Address")
    inn: str = Field(alias="Inn")
    register_time: datetime = Field(alias="RegisterTime")
    last_transactiontime: datetime = Field(alias="LastTransactionTime")
    email: str = Field(alias="Email")
    phone: str = Field(alias="Phone")
    web_site: str = Field(alias="WebSite")
    guid: UUID = Field(alias="Guid")
    change_time: datetime = Field(alias="ChangeTime")
