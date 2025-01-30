from uuid import UUID

from simcc.repositories import conn
from simcc.repositories.util import pagination, webseatch_filter
from simcc.schemas.Production.Article import ArticleMetric


def list_article_metrics(
    researcher_id: UUID,
    program_id: UUID,
    year: int,
) -> list[ArticleMetric]:
    params = {}
    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    program_join = str()
    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_join = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, SUM(opa.citations_count) AS citations,
            ARRAY_AGG(bpa.qualis) AS qualis, ARRAY_AGG(bpa.jcr) AS jcr,
            COUNT(*) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {program_join}
        WHERE 1 = 1
            {program_filter}
            {year_filter}
            {filter_id}
        GROUP BY
            bp.year;
            """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_patent_metrics(researcher_id: UUID, year: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND p.development_year::INT >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT development_year AS year,
            COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS NOT_GRANTED,
            COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS GRANTED
        FROM patent p
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        GROUP BY development_year;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_guidance_metrics(researcher_id: UUID, year: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND g.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND g.year >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT g.year AS year,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT(*) as count_nature
        FROM guidance g
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        GROUP BY g.year, nature, g.status;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_academic_degree_metrics(researcher_id: UUID, year: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND e.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """
            AND (e.education_start >= %(year)s OR e.education_end >= %(year)s)
            """

    SCRIPT_SQL = f"""
        SELECT e.education_start AS year, COUNT(e.degree) AS among,
            REPLACE(degree || '-START', '-', '_') as degree
        FROM education e
        WHERE 1 = 1
            {filter_year}
            {filter_id}
        GROUP BY year, degree

        UNION

        SELECT e.education_end AS year, COUNT(e.degree) AS among,
            REPLACE(degree || '-END', '-', '_') as degree
        FROM education e
        WHERE 1 = 1
            {filter_year}
            {filter_id}
        GROUP BY year, degree
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_software_metrics(researcher_id: UUID, year: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND s.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """AND s.year >= %(year)s"""

    SCRIPT_SQL = f"""
        SELECT s.year, COUNT(*) among
        FROM public.software s
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        GROUP BY s.year;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_patent(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
):
    params = {}
    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('p.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """AND p.development_year::INT >= %(year)s"""

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT p.title AS title, MIN(p.development_year) as year,
            MIN(p.grant_date) AS grant_date, ARRAY_AGG(p.id) AS id,
            NULL AS has_image, NULL AS relevance,
            ARRAY_AGG(DISTINCT r.id) AS researcher,
            ARRAY_AGG(r.lattes_id) AS lattes_id
        FROM patent p
            LEFT JOIN researcher r ON r.id = p.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
        GROUP BY p.title
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_patent(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
):
    params = {}
    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('p.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """AND p.development_year::INT >= %(year)s"""

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT p.title AS title, p.development_year as year,
            p.grant_date AS grant_date, p.id AS id,
            p.has_image AS has_image, p.relevance AS relevance,
            r.id AS researcher, r.lattes_id AS lattes_id
        FROM patent p
            LEFT JOIN researcher r ON r.id = p.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_brand(researcher_id: UUID, year: int, page: int, lenght: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND b.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """AND b.year >= %(year)s"""

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT DISTINCT b.title as title, b.year as year, b.has_image,
            b.relevance, r.lattes_id
        FROM brand b
            LEFT JOIN researcher r
                ON b.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_pagination}
        ORDER BY year desc
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_book(): ...
def list_book(): ...
