from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class GraduateProgram(BaseModel):
    graduate_program_id: UUID
    code: Optional[str]
    name: str
    area: str
    modality: str
    type: Optional[str]
    rating: Optional[str]
    institution_id: UUID
    state: Optional[str]
    city: Optional[str]
    region: Optional[str]
    url_image: Optional[str]
    acronym: Optional[str]
    description: Optional[str]
    visible: Optional[bool]
    site: Optional[str]
