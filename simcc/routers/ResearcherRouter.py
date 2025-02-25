from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ResearcherOptions
from simcc.schemas.Researcher import (
    CoAuthorship,
    ProfessionalExperience,
    Researcher,
)
from simcc.services import ResearcherService

router = APIRouter()


@router.get(
    '/researcher',
    response_model=list[Researcher],
    status_code=HTTPStatus.OK,
)
def search_in_abstract_or_article(
    terms: str = None,
    type: ResearcherOptions = 'ABSTRACT',
    graduate_program_id: UUID | str = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
):
    if type == 'ARTICLE':
        researchers = ResearcherService.search_in_articles(
            terms, graduate_program_id, university, page, lenght
        )
    elif type == 'ABSTRACT':
        researchers = ResearcherService.search_in_abstracts(
            terms, graduate_program_id, university
        )
    return researchers


@router.get(
    '/researcherName',
    response_model=list[Researcher],
    status_code=HTTPStatus.OK,
)
def list_researchers(
    name: str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherService.serch_in_name(
        name, graduate_program_id, dep_id, page, lenght
    )


@router.get(
    '/researcher/co-authorship/{researcher_id}',
    status_code=HTTPStatus.OK,
    response_model=list[CoAuthorship],
)
def co_authorship(researcher_id: UUID):
    return ResearcherService.list_co_authorship(researcher_id)


@router.get(
    '/professional_experience',
    response_model=list[ProfessionalExperience],
)
def get_professional_experience(
    researcher_id: UUID = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    year: int = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherService.professional_experience(
        researcher_id, graduate_program_id, dep_id, year, page, lenght
    )
