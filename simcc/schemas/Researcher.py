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
    BC: int
    citations: int
