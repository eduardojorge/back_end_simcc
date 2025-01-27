from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas.Brand import BrandProduction
from simcc.schemas.Patent import PatentProduction
from simcc.services import ProductionService

router = APIRouter()


@router.get(
    '/patent_production_researcher',
    response_model=list[PatentProduction],
    status_code=HTTPStatus.OK,
)
def list_patent_production(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2000,
    distinct: int = 1,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        patents = ProductionService.list_distinct_patent(
            term, researcher_id, year, page, lenght
        )
    else:
        patents = ProductionService.list_patent(
            term, researcher_id, year, page, lenght
        )
    return patents


@router.get(
    '/brand_production_researcher',
    response_model=list[BrandProduction],
    status_code=HTTPStatus.OK,
)
def list_brand_production(
    researcher_id: UUID | str = None,
    year: int | str = 2000,
    page: int = None,
    lenght: int = None,
):
    brands = ProductionService.list_brand(researcher_id, year, page, lenght)
    return brands
