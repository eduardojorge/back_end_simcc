from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class Qualis(str, Enum):
    A1 = 'A1'
    A2 = 'A2'
    B1 = 'B1'
    B2 = 'B2'
    B3 = 'B3'
    B4 = 'B4'
    B5 = 'B5'
    C = 'C'
    SQ = 'SQ'  # Sem Qualis


class Article(BaseModel):
    id: UUID
    title: str
    year: str
    type: str
    doi: Optional[str] = None
    qualis: Optional[str]
    magazine: Optional[str]
    researcher: str
    lattes_10_id: str
    lattes_id: str
    jif: Optional[float] = None
    jcr_link: Optional[str] = None
    researcher_id: UUID
