from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Query

from ..dao import dao_article
from ..model.article import Article

router = APIRouter(tags=['Research Search'])

default_year = 2014

term_description = """
A string containing the search terms that will be used to filter articles.
You can use operators like ';' (AND), '.' (AND NOT), '|' (OR), and parentheses
for grouping.
For example: 'data science | machine learning AND AI'.
Terms will be processed for full-text search in the specified column.
"""


@router.get(
    '/bibliographic_production_researcher',
    response_model=list[Article],
    status_code=HTTPStatus.OK,
)
def article_search(
    researcher_id: UUID = None,
    qualis: str = None,
    year: int = default_year,
    type: str = None,
    terms: str = Query(..., description=term_description),
):
    if type == 'ARTICLE':
        article_list = dao_article.article_search(researcher_id, qualis, year, terms)
    if type == 'ABSTRACT':
        ...

    return article_list
