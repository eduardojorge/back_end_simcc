from uuid import UUID

from pydantic import BaseModel

from simcc.schemas import QualisOptions


class Qualis(BaseModel):
    A1: int = 0
    A2: int = 0
    A3: int = 0
    A4: int = 0
    B1: int = 0
    B2: int = 0
    B3: int = 0
    B4: int = 0
    C: int = 0
    SQ: int = 0


class Jcr(BaseModel):
    very_low: int
    low: int
    medium: int
    high: int
    not_applicable: int
    without_jcr: int


class ArticleMetric(BaseModel):
    year: int
    among: int
    qualis: Qualis
    jcr: Jcr
    citations: int | None


class Article(BaseModel):
    # Article Data
    id: UUID
    title: str
    year: str
    doi: str
    qualis: str
    magazine: str
    jcr: float
    jcr_link: str

    # OpenAlex Data
    article_institution: str
    issn: str
    authors_institution: str
    abstract: str
    authors: str
    language: str
    citations_count: str
    pdf: str
    landing_page_url: str
    keywords: str

    # Researcher Data
    researcher: str
    researcher_id: UUID
    lattes_id: str


class ArticleProduction(BaseModel):
    id: UUID
    title: str
    year: int
    type: str
    doi: str | None
    qualis: QualisOptions
    magazine: str
    researcher: str
    lattes_10_id: str
    lattes_id: str
    jif: str | None
    jcr_link: str | None
    researcher_id: UUID
    lattes_id: str

    abstract: str | None
    article_institution: str | None
    authors: str | None
    authors_institution: str | None
    citations_count: int | str | None
    issn: str | None
    keywords: str | None
    landing_page_url: str | None
    language: str | None
    pdf: str | None

    has_image: bool | None
    relevance: bool | None
