from typing import Annotated

from fastapi import Query


class PaginateQueryParams:
    """Dependency class to parse pagination query params."""

    def __init__(
        self,
        page: Annotated[
            int,
            Query(
                title="Номер страницы",
                description="Номер возвращаемой страницы",
                ge=1,
            ),
        ] = 1,
        page_size: Annotated[
            int,
            Query(
                title="Размер страницы",
                description="Число элементов на странице",
                ge=1,
                le=500,
            ),
        ] = 20,
    ):
        self.page = page
        self.page_size = page_size
