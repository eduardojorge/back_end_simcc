from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas.Article import ArticleMetric
from simcc.schemas.GraduateProgram import GraduateProgram
from simcc.schemas.Researcher import ResearcherArticleProduction
from simcc.services import GraduateProgramService, ProductionService

router = APIRouter()


@router.get(
    '/',
    response_model=list[GraduateProgram],
    status_code=HTTPStatus.OK,
)
def list_graduate_programs():
    graduate_programs = GraduateProgramService.list_graduate_programs()
    return graduate_programs


@router.get(
    '/{program_id}/article_production',
    response_model=list[ResearcherArticleProduction],
    status_code=HTTPStatus.OK,
)
def article_production(program_id: UUID, year: int = 2020):
    article_production_list = GraduateProgramService.list_article_production(
        program_id, year
    )
    return article_production_list


@router.get(
    '/{program_id}/article_metrics',
    response_model=list[ArticleMetric],
    status_code=HTTPStatus.OK,
)
def article_metrics(program_id: UUID, year: int = 2020):
    article_metrics_list = ProductionService.list_article_metrics(
        None, program_id, year
    )
    return article_metrics_list
