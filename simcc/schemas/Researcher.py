from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class ResearcherArticleProduction(BaseModel):
    name: str
    A1: int
    A2: int
    A3: int
    A4: int
    B1: int
    B2: int
    B3: int
    B4: int
    C: int
    SQ: int
    citations: int
    year: int


class Researcher(BaseModel):
    # Researcher Data
    id: UUID
    name: str
    lattes_id: str
    lattes_10_id: str
    university: str
    abstract: str | None
    area: str
    city: str
    image_university: str | None
    orcid: str | None
    graduation: str
    lattes_update: datetime

    # Metrics
    among: int | None
    articles: int
    book_chapters: int
    book: int
    patent: int
    software: int
    brand: int

    # OpenAlex Data
    h_index: int | None
    relevance_score: int | None
    works_count: int | None
    cited_by_count: int | None
    i10_index: int | None
    scopus: str | None
    openalex: str | None

    class Config:
        json_encoders = {datetime: lambda v: v.strftime('%d/%m/%Y')}
