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
    descr: str = Field(alias="Descr")
    address: str = Field(alias="Address")
    inn: str = Field(alias="Inn")
    email: str = Field(alias="Email")
    phone: str = Field(alias="Phone")
    web_site: str = Field(alias="WebSite")
    # register_time: datetime = Field(alias="RegisterTime")
    # last_transactiontime: datetime = Field(alias="LastTransactionTime")
    # type_: int = Field(alias="Type")
    # is_stuff: bool = Field(alias="IsStuff")
    # is_blocked: bool = Field(alias="IsBlocked")
    # guid: UUID = Field(alias="Guid")
    # change_time: datetime = Field(alias="ChangeTime")


class TotalReportElement(Model):
    """Элемент итогового отчета."""

    super_name: str = Field(alias="SuperName")
    view_string: str | None = Field(alias="ViewString")
    name: str = Field(alias="Name")
    good_amount: int | None = Field(alias="GoodAmount")
    amount: float | None = Field(alias="Amount")
    # super_parent_id: int = Field(alias="SuperParentId")
    # good_stock_id_from: int = Field(alias="GoodStockIdFrom")
    # lookup_interface_id: int | None = Field(alias="LookupInterfaceId")


class TotalReport(Model):
    elements: list[TotalReportElement] = Field(default_factory=list)

    @property
    def total_sum(self) -> TotalReportElement:
        """Расчет 'Итого по отчету'."""

        amount = 0
        good_amount = 0.0
        for element in self.elements:
            if (
                element.name == "Депозит"
                or element.good_amount is None
                or element.amount is None
            ):
                continue

            good_amount += element.good_amount
            amount += element.amount

        return TotalReportElement(
            super_name="Итого",
            view_string="Итого",
            name="Итого по отчету",
            good_amount=good_amount,
            amount=amount,
        )


class ClientsCount(Model):
    count: int
    id: int
    zone_name: str
    code: str
