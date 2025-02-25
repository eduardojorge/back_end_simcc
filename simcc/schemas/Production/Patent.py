from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class PatentMetric(BaseModel):
    year: int
    granted: int
    not_granted: int


class PatentProduction(BaseModel):
    # Distinct 1
    title: str
    researcher: UUID | list[UUID]
    grant_date: datetime | None
    year: int
    name: str | list[str]

    # Distinct 0
    id: UUID | list[UUID]
    lattes_id: str | list[str]
    patent: None = None
    has_image: bool | None
    relevance: bool | None
