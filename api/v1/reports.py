import logging
from datetime import date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query, HTTPException

from constants import gen_db_name_enum
from core.settings import settings
from gateways.telegram import TelegramBot, get_telegram_bot
from legacy.barsicreport2 import BarsicReport2Service, get_legacy_service
from services.bars import BarsService, get_bars_service
from services.workers import WorkerService, get_worker_service

logger = logging.getLogger(__name__)


router = APIRouter()


@router.post("/client_count")
async def client_count(
    legacy_service: Annotated[BarsicReport2Service, Depends(get_legacy_service)],
) -> dict:
    """Количество людей в зоне."""

    return legacy_service.count_clients_print()


@router.post("/create_reports")
async def create_reports(
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()) + timedelta(days=1),
    use_yadisk: bool = False,
    telegram_report: bool = False,
    legacy_service: BarsicReport2Service = Depends(get_legacy_service),
) -> dict:
    """Создание всех отчетов."""

    await legacy_service.run_report(
        date_from=date_from,
        date_to=date_to,
        use_yadisk=use_yadisk,
        telegram_report=telegram_report,
    )

    return {
        "ok": True,
        "Google Report": legacy_service.spreadsheet["spreadsheetUrl"],
    }


@router.post("/send_telegram")
async def send_telegram(
    message: str,
    telegram_bot: Annotated[TelegramBot, Depends(get_telegram_bot)],
) -> dict:
    """Отправить сообщение в телеграм."""

    message = await telegram_bot.send_message(settings.telegram_chanel_id, message)
    return {"message": message.text}


@router.post("/create_total_report_by_day")
async def create_total_report_by_day(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()),
    use_cache: bool = True,
    bars_service: BarsService = Depends(get_bars_service),
    worker_service: WorkerService = Depends(get_worker_service),
) -> dict:
    """Список Организаций."""

    if date_from >= date_to:
        raise HTTPException(status_code=404, detail="date_from >= date_to")

    bars_service.choose_db(db_name=db_name.value)
    return await worker_service.get_total_report_with_groups(date_from, date_to, use_cache=use_cache)


@router.post("/create_purchased_goods_report")
async def create_purchased_goods_report(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()),
    goods: list[str] = Query(default_factory=list),
    use_like: bool = False,
    save_to_yandex: bool = False,
    hide_zero: bool = False,
    worker_service: WorkerService = Depends(get_worker_service),
) -> dict:
    """Список Организаций."""

    worker_service.choose_db(db_name=db_name.value)
    return await worker_service.create_purchased_goods_report(
        date_from=date_from,
        date_to=date_to,
        goods=goods,
        use_like=use_like,
        save_to_yandex=save_to_yandex,
        hide_zero=hide_zero,
    )


@router.post("/create_attendance_report")
async def create_attendance_report(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()),
    save_to_yandex: bool = False,
    save_to_google: bool = True,
    use_cache: bool = True,
    worker_service: WorkerService = Depends(get_worker_service),
) -> dict:
    """Список Организаций."""

    if not save_to_google and not save_to_yandex:
        raise HTTPException(status_code=404, detail="At least one of 'save_to_google' or 'save_to_yandex' must be True")

    if date_from >= date_to:
        raise HTTPException(status_code=404, detail="date_from >= date_to")

    worker_service.choose_db(db_name=db_name.value)
    return await worker_service.create_attendance_report(
        date_from=date_from,
        date_to=date_to,
        save_to_yandex=save_to_yandex,
        save_to_google=save_to_google,
        use_cache=use_cache,
    )
