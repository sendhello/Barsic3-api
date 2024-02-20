from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends

from core.settings import settings
from gateways.telegram import send_message
from legacy.barsicreport2 import BarsicReport2Service, get_legacy_service


router = APIRouter()


@router.post("/client_count", response_model=dict)
async def client_count(
    legacy_service: BarsicReport2Service = Depends(get_legacy_service),
) -> dict:
    """Количество людей в зоне."""

    client_count = legacy_service.count_clients_print()
    return client_count


@router.post("/create_reports", response_model=dict)
async def create_reports(
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time())
    + timedelta(days=1),
    use_yadisk: bool = False,
    telegram_report: bool = False,
    legacy_service: BarsicReport2Service = Depends(get_legacy_service),
) -> dict:
    """Создание всех отчетов."""

    legacy_service.run_report(
        date_from=date_from,
        date_to=date_to,
        use_yadisk=use_yadisk,
    )

    # Отправка Telegram отчета
    message = None
    if telegram_report:
        for message in legacy_service.sms_report_list:
            message = await send_message(settings.telegram_chanel_id, message)

    return {
        "ok": True,
        "Google Report": legacy_service.spreadsheet["spreadsheetUrl"],
        "Telegram Message": message.text if message else None,
    }


@router.post("/send_telegram", response_model=dict)
async def send_telegram(
    message: str, legacy_service: BarsicReport2Service = Depends(get_legacy_service)
) -> dict:
    """Отправить сообщение в телеграм."""

    message = await send_message(settings.telegram_chanel_id, message)
    return {"message": message.text}
