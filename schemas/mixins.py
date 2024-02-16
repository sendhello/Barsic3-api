from uuid import UUID

from pydantic import BaseModel


class IdMixin(BaseModel):
    id: UUID
