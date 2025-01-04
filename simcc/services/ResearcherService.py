from uuid import UUID

from simcc.repositories.simcc import ResearcherRepository
from simcc.schemas.Researcher import Researcher


def search_in_articles(
    terms: str = None,
    graduate_program_id: UUID = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_articles(
        terms, graduate_program_id, university, page, lenght
    )
    return researchers


def search_in_abstracts(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int = None,
    lenght: int = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_abstracts(
        terms, graduate_program_id, university, page, lenght
    )
    return researchers
