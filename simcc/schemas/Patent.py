from pydantic import BaseModel


class PatentMetric(BaseModel):
    year: int
    granted: int
    not_granted: int
