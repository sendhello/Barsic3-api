import orjson
from pydantic import BaseModel, ConfigDict


def orjson_dumps(v, *, default):
    # orjson.dumps возвращает bytes, а pydantic требует unicode, поэтому декодируем
    return orjson.dumps(v, default=default).decode()


class Model(BaseModel):
    """Базовая модель."""

    model_config = ConfigDict(from_attributes=True)
