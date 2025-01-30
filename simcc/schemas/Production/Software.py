from pydantic import BaseModel


class SoftwareMetric(BaseModel):
    year: int
    among: int
