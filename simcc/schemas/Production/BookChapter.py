from uuid import UUID

from pydantic import BaseModel


class BookChapterProduction(BaseModel):
    title: str
    year: int
    isbn: str | None
    publishing_company: str | None

    id: UUID | list[UUID]
    researcher: UUID | list[UUID]
    lattes_id: str | list[str]
    name: str | list[str]

    has_image: bool | None
    relevance: bool | None
