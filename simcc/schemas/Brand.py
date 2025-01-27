from pydantic import BaseModel


class BrandProduction(BaseModel):
    title: str
    year: int
    has_image: bool
    relevance: bool
    lattes_id: str
