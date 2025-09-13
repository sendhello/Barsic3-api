from datetime import datetime
from decimal import Decimal
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


class MasterTransactionBase(Model):
    master_transaction_id: int = Field(alias="MasterTransactionId")
    trans_time: datetime = Field(alias="TransTime")
    super_account_from: int | None = Field(alias="SuperAccountFrom")
    super_account_to: int | None = Field(alias="SuperAccountTo")
    user_id: str = Field(alias="UserId")
    service_point_id: int | None = Field(alias="ServicePointId")
    server_time: datetime | None = Field(alias="ServerTime")
    check_detail_id: int | None = Field(alias="CheckDetailId")
    external_id: str | None = Field(alias="ExternalId")
    machine: str | None = Field(alias="Machine")
    sec_subject_id: int | None = Field(alias="SecSubjectId")
    guid: UUID | None = Field(alias="Guid")
    extended_data: str | None = Field(alias="ExtendedData")


class CheckDetailBase(Model):
    check_id: int | None = Field(alias="CheckId")
    name: str | None = Field(alias="Name")
    count: Decimal | None = Field(alias="Count")
    price: Decimal | None = Field(alias="Price")
    card_code: str | None = Field(alias="CardCode")
    category_id: int | None = Field(alias="CategoryId")
    type_good: int | None = Field(alias="TypeGood")
    account: int | None = Field(alias="Account")


class ClientTransaction(MasterTransactionBase, CheckDetailBase):
    super_account: int | None = Field(alias="SuperAccount")


class ExtendedService(Model):
    name: str
    count: int = Field(default_factory=int)
    summ: Decimal = Field(default_factory=Decimal)
