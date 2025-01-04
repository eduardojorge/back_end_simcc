from uuid import UUID

from simcc.repositories import conn
from simcc.repositories.util import pagination, web_search_filter
from simcc.schemas.Researcher import ResearcherArticleProduction


def list_article_production(
    program_id: UUID, year: int
) -> list[ResearcherArticleProduction]:
    params = {}

    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT r.name, bpa.qualis, COUNT(*) AS among, bp.year,
            COALESCE(SUM(opa.citations_count), 0) AS citations
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        WHERE 1 = 1
            {program_filter}
            {year_filter}
        GROUP BY r.id, bpa.qualis, bp.year
        HAVING 1 = 1;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_articles(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int,
    lenght: int,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        params['terms'] = web_search_filter(terms)
        filter_terms = """
            AND to_tsvector(translate(unaccent(LOWER(r.abstract)), '-.:;''', ' ')),
                websearch_to_tsquery(%(terms)s)) > 0.04
            """  # noqa: E501

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            RIGHT JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    filter_institution = str()
    if university:
        params['institution']
        filter_institution = 'AND i.name = %(institution)s'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update, rp.great_area AS area,
            rp.city, i.image AS image_university, i.name AS university,
            b.among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            RIGHT JOIN
                (SELECT b.researcher_id, COUNT(*) AS among
                FROM bibliographic_production b
                WHERE b.type = 'ARTICLE'
                    {filter_terms}
                GROUP BY researcher_id) b ON b.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
        WHERE 1 = 1
            {filter_program}
            {filter_institution}
        ORDER BY
            among DESC
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_abstracts(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        params['terms'] = web_search_filter(terms)
        filter_terms = r"""
            AND to_tsvector(translate(unaccent(LOWER(r.abstract)), '-.:;''', ' ')),
                websearch_to_tsquery(%(terms)s)) > 0.04
            """  # noqa: E501

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            RIGHT JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    filter_institution = str()
    if university:
        params['institution']
        filter_institution = 'AND i.name = %(institution)s'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update, rp.great_area AS area,
            rp.city, i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
        WHERE 1 = 1
            {filter_terms}
            {filter_program}
            {filter_institution}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result
