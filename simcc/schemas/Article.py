from uuid import UUID

from pydantic import BaseModel


class ArticleMetric(BaseModel):
    id: UUID
    jcr: float | None
    qualis: str
    citations: int | None
    year: int


class Article(BaseModel):
    # Article Data
    id: UUID
    title: str
    year: str
    doi: str
    qualis: str
    magazine: str
    jcr: str
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
