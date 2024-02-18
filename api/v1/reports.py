from fastapi import APIRouter, Depends

from legacy.barsicreport2 import get_legacy_service, BarsicReport2Service

router = APIRouter()


@router.post("/client_count", response_model=dict)
async def client_count(legacy_service: BarsicReport2Service = Depends(get_legacy_service)) -> dict:
    """Количество людей в зоне."""

    client_count = legacy_service.count_clients_print()
    return client_count
