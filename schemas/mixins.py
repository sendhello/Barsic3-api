from uuid import UUID

from pydantic import BaseModel, Field


class IdMixin(BaseModel):
    id: UUID = Field(description="ID")
