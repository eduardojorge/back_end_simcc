from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas.Production.Article import ArticleMetric
from simcc.schemas.Production.Guidance import GuidanceMetrics
from simcc.schemas.Production.Patent import PatentMetric
from simcc.schemas.Production.Software import SoftwareMetric
from simcc.schemas.Researcher import AcademicMetric
from simcc.services import ProductionService

router = APIRouter()


@router.get(
    '/article_metrics',
    response_model=list[ArticleMetric],
    status_code=HTTPStatus.OK,
    tags=['Metrics'],
)
def article_metrics(program_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_article_metrics(None, program_id, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/article_metrics',
    response_model=list[ArticleMetric],
    status_code=HTTPStatus.OK,
    tags=['Researcher'],
)
def article_metrics_researcher(researcher_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_article_metrics(researcher_id, None, year)
    return metrics


@router.get(
    '/graduate_program/{program_id}/article_metrics',
    response_model=list[ArticleMetric],
    status_code=HTTPStatus.OK,
    tags=['Graduate Program'],
)
def article_metrics_graduate_program(program_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_article_metrics(None, program_id, year)
    return metrics


@router.get(
    '/patent_metrics',
    response_model=list[PatentMetric],
    status_code=HTTPStatus.OK,
    tags=['Metrics'],
)
def patent_metrics(year: int = 2020):
    metrics = ProductionService.list_patent_metrics(None, None, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/patent_metrics',
    response_model=list[PatentMetric],
    status_code=HTTPStatus.OK,
    tags=['Researcher'],
)
def patent_metrics_researcher(researcher_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_patent_metrics(researcher_id, None, year)
    return metrics


@router.get(
    '/graduate_program/{program_id}/patent_metrics',
    response_model=list[PatentMetric],
    status_code=HTTPStatus.OK,
    tags=['Graduate Program'],
)
def patent_metrics_graduate_program(program_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_patent_metrics(None, program_id, year)
    return metrics


@router.get(
    '/guidance_metrics',
    response_model=list[GuidanceMetrics],
    status_code=HTTPStatus.OK,
    tags=['Metrics'],
)
def guidance_metrics(year: int = 2020):
    metrics = ProductionService.list_guidance_metrics(None, None, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/guidance_metrics',
    response_model=list[GuidanceMetrics],
    status_code=HTTPStatus.OK,
    tags=['Researcher'],
)
def guidance_metrics_researcher(researcher_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_guidance_metrics(researcher_id, None, year)
    return metrics


@router.get(
    '/graduate_program/{program_id}/guidance_metrics',
    response_model=list[GuidanceMetrics],
    status_code=HTTPStatus.OK,
    tags=['Graduate Program'],
)
def guidance_metrics_graduate_program(program_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_guidance_metrics(None, program_id, year)
    return metrics


@router.get(
    '/academic_degree_metrics',
    response_model=list[AcademicMetric],
    status_code=HTTPStatus.OK,
    tags=['Metrics'],
)
def academic_degree_metrics(year: int = 2020):
    metrics = ProductionService.list_academic_degree_metrics(None, None, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/academic_degree_metrics',
    response_model=list[AcademicMetric],
    status_code=HTTPStatus.OK,
    tags=['Researcher'],
)
def academic_degree_metrics_researcher(
    researcher_id: UUID = None, year: int = 2020
):
    metrics = ProductionService.list_academic_degree_metrics(
        researcher_id, None, year
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/academic_degree_metrics',
    response_model=list[AcademicMetric],
    status_code=HTTPStatus.OK,
    tags=['Graduate Program'],
)
def academic_degree_metrics_graduate_program(
    program_id: UUID = None, year: int = 2020
):
    metrics = ProductionService.list_academic_degree_metrics(
        None, program_id, year
    )
    return metrics


@router.get(
    '/software_metrics',
    response_model=list[SoftwareMetric],
    status_code=HTTPStatus.OK,
    tags=['Metrics'],
)
def software_metrics(year: int = 2020):
    metrics = ProductionService.list_software_metrics(None, None, year)
    return metrics


@router.get(
    '/researcher/{researcher_id}/software_metrics',
    response_model=list[SoftwareMetric],
    status_code=HTTPStatus.OK,
    tags=['Researcher'],
)
def software_metrics_researcher(researcher_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_software_metrics(researcher_id, None, year)
    return metrics


@router.get(
    '/graduate_program/{program_id}/software_metrics',
    response_model=list[SoftwareMetric],
    status_code=HTTPStatus.OK,
    tags=['Graduate Program'],
)
def software_metrics_graduate_program(program_id: UUID = None, year: int = 2020):
    metrics = ProductionService.list_software_metrics(None, program_id, year)
    return metrics
