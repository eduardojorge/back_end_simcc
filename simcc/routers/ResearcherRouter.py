from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ResearcherOptions
from simcc.schemas.Researcher import Researcher
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
