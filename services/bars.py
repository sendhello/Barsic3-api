from repositories.bars import BarsRepository, get_bars_repo
from schemas.bars import Category, Organisation


class BarsService:
    def __init__(self, repository: BarsRepository):
        self._repo = repository

    def get_tariffs(self, db_name: str, organization_id: int) -> list[Category]:
        tariffs = self._repo.get_tariffs(db_name, organization_id)
        return [Category.model_validate(tariff) for tariff in tariffs]

    def get_organisations(self, db_name: str) -> list[Organisation]:
        organisations = self._repo.get_organisations(db_name)
        return [Organisation.model_validate(org) for org in organisations]


def get_bars_service():
    return BarsService(repository=get_bars_repo())
