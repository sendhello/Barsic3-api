from pydantic import BaseModel, ConfigDict


class Model(BaseModel):
    """Базовая модель."""

    model_config = ConfigDict(from_attributes=True)
