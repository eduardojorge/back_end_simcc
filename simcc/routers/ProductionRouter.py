from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas.Book import BookProduction
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
    year: int | str = 2020,
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
    '/book_production_researcher',
    response_model=list[BookProduction],
    status_code=HTTPStatus.OK,
)
def list_book_production(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    distinct: int = 1,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        books = ProductionService.list_distinct_book(
            term, researcher_id, year, page, lenght
        )
    else:
        books = ProductionService.list_book(
            term, researcher_id, year, page, lenght
        )
    return books


@router.get(
    '/brand_production_researcher',
    response_model=list[BrandProduction],
    status_code=HTTPStatus.OK,
)
def list_brand_production(
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    page: int = None,
    lenght: int = None,
):
    brands = ProductionService.list_brand(researcher_id, year, page, lenght)
    return brands
