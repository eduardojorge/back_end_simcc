from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ResearcherOptions
from simcc.schemas.Article import ArticleMetric
from simcc.schemas.Guidance import GuidanceMetrics
from simcc.schemas.Patent import PatentMetric
from simcc.schemas.Researcher import AcademicMetric, Researcher
from simcc.schemas.Software import SoftwareMetric
from simcc.services import ProductionService, ResearcherService

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
    '/researcher/{researcher_id}/article_metrics',
    response_model=list[ArticleMetric],
    status_code=HTTPStatus.OK,
)
def article_metrics(researcher_id: UUID, year: int = 2020):
    metrics = ProductionService.list_article_metrics(researcher_id, None, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/patent_metrics',
    response_model=list[PatentMetric],
    status_code=HTTPStatus.OK,
)
def patent_metrics(researcher_id: UUID, year: int = 2020):
    metrics = ProductionService.list_patent_metrics(researcher_id, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/guidance_metrics',
    response_model=list[GuidanceMetrics],
    status_code=HTTPStatus.OK,
)
def guidance_metrics(researcher_id: UUID, year: int = 2020):
    metrics = ProductionService.list_guidance_metrics(researcher_id, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/academic_degree_metrics',
    response_model=list[AcademicMetric],
    status_code=HTTPStatus.OK,
)
def academic_degree_metrics(researcher_id: UUID, year: int = 2020):
    metrics = ProductionService.list_academic_degree_metrics(researcher_id, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/software_metrics',
    response_model=list[SoftwareMetric],
    status_code=HTTPStatus.OK,
)
def software_metrics(researcher_id: UUID, year: int = 2020):
    metrics = ProductionService.list_software_metrics(researcher_id, year)
    return metrics


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
    researchers = ResearcherService.serch_in_name(
        name, graduate_program_id, dep_id, page, lenght
    )
    return researchers
