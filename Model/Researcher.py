from typing import Optional
from pydantic import BaseModel, UUID4


class Researcher(BaseModel):
    id: UUID4
    name: Optional[str]
    among: Optional[str]
    articles: Optional[str]
    book_chapters: Optional[str]
    book: Optional[str]
    patent: Optional[str]
    software: Optional[str]
    brand: Optional[str]
    university: Optional[str]
    lattes_id: Optional[str]
    lattes_10_id: Optional[str]
    abstract: Optional[str]
    area: Optional[str]
    city: Optional[str]
    orcid: Optional[str]
    image: Optional[str]
    graduation: Optional[str]
    lattes_update: Optional[str]
