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
    abstract: str
    area: str
    city: str
    image_university: str
    orcid: str | str
    graduation: str
    lattes_update: datetime
    classification: str
    status: bool

    # Metrics
    among: int | str
    articles: int | str
    book_chapters: int | str
    book: int | str
    patent: int | str
    software: int | str
    brand: int | str

    # OpenAlex Data
    h_index: int | str
    relevance_score: int | str
    works_count: int | str
    cited_by_count: int | str
    i10_index: int | str
    scopus: str | str
    openalex: str | str

    # Miscellaneous
    research_groups: list | str
    subsidy: list | str
    departments: list | str
    graduate_programs: list | str

    # UFMG
    matric: str
    inscufmg: str
    genero: str
    situacao: str
    rt: str
    clas: str
    cargo: str
    classe: str
    ref: str
    titulacao: str
    entradanaufmg: datetime | str
    progressao: datetime | str
    semester: str

    class Config:
        json_encoders = {datetime: lambda v: v.strftime('%d/%m/%Y')}


class AcademicMetric(BaseModel):
    year: int
    doctorate_end: int
    doctorate_start: int
    masters_degree_end: int
    masters_degree_start: int
    undergraduate_end: int
    undergraduate_start: int
    specialization_start: int
    specialization_end: int
    professional_masters_degree_start: int
    professional_masters_degree_end: int


class CoAuthorship(BaseModel):
    id: UUID | None
    name: str
    among: int
    type: str
