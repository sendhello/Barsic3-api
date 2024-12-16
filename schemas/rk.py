from pydantic import field_validator

from .base import Model


class SmileReport(Model):
    total_count: float
    total_sum: float

    @field_validator("total_count", "total_sum", mode="before")
    @classmethod
    def float_validator(cls, v):
        return float(v or 0)
