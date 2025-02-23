from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ArticleOptions, QualisOptions
from simcc.schemas.Production.Article import ArticleProduction
from simcc.schemas.Production.Book import BookProduction
from simcc.schemas.Production.BookChapter import BookChapterProduction
from simcc.schemas.Production.Brand import BrandProduction
from simcc.schemas.Production.Patent import PatentProduction
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
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        patents = ProductionService.list_distinct_patent(
            term, researcher_id, year, institution_id, page, lenght
        )
    else:
        patents = ProductionService.list_patent(
            term, researcher_id, year, institution_id, page, lenght
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
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        books = ProductionService.list_distinct_book(
            term, researcher_id, year, institution_id, page, lenght
        )
    else:
        books = ProductionService.list_book(
            term, researcher_id, year, institution_id, page, lenght
        )
    return books


@router.get(
    '/brand_production_researcher',
    response_model=list[BrandProduction],
    status_code=HTTPStatus.OK,
)
def list_brand_production(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    brands = ProductionService.list_brand(
        term, researcher_id, year, institution_id, page, lenght
    )
    return brands


@router.get(
    '/book_chapter_production_researcher',
    response_model=list[BookChapterProduction],
    status_code=HTTPStatus.OK,
)
def list_book_chapter_production(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    distinct: int = 1,
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        books = ProductionService.list_distinct_book_chapter(
            term, researcher_id, year, institution_id, page, lenght
        )
    else:
        books = ProductionService.list_book_chapter(
            term, researcher_id, year, institution_id, page, lenght
        )
    return books


@router.get(
    '/bibliographic_production_researcher',
    response_model=list[ArticleProduction],
    status_code=HTTPStatus.OK,
)
def list_bibliographic_production(
    terms: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions | str = str(),
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    articles = ProductionService.list_bibliographic_production(
        terms, researcher_id, year, type, qualis, institution_id, page, lenght
    )
    return articles


@router.get(
    '/bibliographic_production_article',
    response_model=list[ArticleProduction],
    status_code=HTTPStatus.OK,
)
def list_article_production(
    terms: str = None,
    university: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions | str = str(),
    distinct: int = 1,
    page: int = None,
    lenght: int = None,
    dep_id: str = None,
):
    if distinct:
        articles = ProductionService.list_distinct_article_production(
            terms,
            university,
            researcher_id,
            graduate_program_id,
            year,
            type,
            qualis,
            page,
            lenght,
            dep_id,
        )
    else:
        articles = ProductionService.list_article_production(
            terms,
            university,
            researcher_id,
            graduate_program_id,
            year,
            type,
            qualis,
            page,
            lenght,
            dep_id,
        )
    return articles
