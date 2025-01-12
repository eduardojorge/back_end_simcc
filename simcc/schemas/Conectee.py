from datetime import datetime

from pydantic import BaseModel, Field


class ResearcherData(BaseModel):
    nome: str = Field(..., max_length=255)
    cpf: str = Field(..., max_length=14)
    classe: int
    nivel: int
    inicio: datetime
    fim: datetime | None
    tempo_nivel: int | None
    tempo_acumulado: int
    arquivo: str = Field(..., max_length=255)
