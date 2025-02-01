from uuid import UUID

from simcc.repositories import conn
from simcc.repositories.util import pagination, webseatch_filter
from simcc.schemas import ArticleOptions, QualisOptions
from simcc.schemas.Production.Article import (
    ArticleMetric,
)


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
        filter_year = 'AND p.development_year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT p.title AS title, MAX(p.development_year) as year,
            MAX(p.grant_date) AS grant_date, ARRAY_AGG(p.id) AS id,
            NULL AS has_image, NULL AS relevance,
            ARRAY_AGG(r.id) AS researcher,
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
        filter_year = 'AND p.development_year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT p.title AS title, p.development_year as year,
            p.grant_date AS grant_date, p.id AS id,
            p.has_image, p.relevance,
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


def list_brand(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND b.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND b.year >= %(year)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('b.title', term)
        params |= term

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
            {filter_terms}
        ORDER BY year desc
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_book(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('bp.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT bp.title, year, bpb.isbn AS isbn,
            MAX(bpb.publishing_company) AS publishing_company,
            ARRAY_AGG(bp.researcher_id) AS researcher,
            ARRAY_AGG(r.lattes_id) AS lattes_id, NULL AS relevance,
            NULL AS has_image, ARRAY_AGG(bp.id) AS id
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book bpb
                ON bp.id = bpb.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_terms}
            {filter_year}
        GROUP BY bp.title, bpb.isbn, year
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_book(term: str, researcher_id: UUID, year: int, page: int, lenght: int):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('bp.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT bp.title, year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image , bp.id
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book bpb
                ON bp.id = bpb.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_terms}
            {filter_year}
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_bibliographic_production(
    terms: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('b.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis
        filter_qualis = 'AND bpa.qualis = %(qualis)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT DISTINCT
            b.id AS id, title, year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors, opa.authors_institution,
            COALESCE (opa.citations_count, 0) AS citations_count, bpa.issn,
            opa.keywords, opa.landing_page_url, opa.language, opa.pdf,
            b.has_image, b.relevance
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_pagination}
        ORDER BY
            year DESC
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_article_production(  # noqa: PLR0914
    terms: str = None,
    university: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    page: int = None,
    lenght: int = None,
    dep_id: str = None,
):
    params = {}

    filter_university = str()
    join_university = str()
    if university:
        join_university = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        filter_university = 'AND i.name = %(university)s'
        params['university'] = university

    filter_program = str()
    join_program = str()
    if graduate_program_id:
        params['program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON r.id = gpr.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('bp.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis
        filter_qualis = 'AND bpa.qualis = %(qualis)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_dep = str()
    join_dep = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_dep = """
            INNER JOIN ufmg.departament_researcher dr
                ON r.id = dr.researcher_id
            """
        filter_dep = 'AND dr.dep_id = %(dep_id)s'

    SCRIPT_SQL = f"""
        SELECT
            b.id AS id, title, b.year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors,
            opa.authors_institution, opa.citations_count, bpa.issn, opa.keywords,
            opa.landing_page_url, opa.language, opa.pdf, b.has_image, b.relevance
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_university}
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_pagination}
            {filter_program}
            {filter_university}
            {filter_dep}
        ORDER BY
            year DESC
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_article_production(  # noqa: PLR0914
    terms: str = None,
    university: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    page: int = None,
    lenght: int = None,
    dep_id: str = None,
):
    params = {}

    filter_university = str()
    join_university = str()
    if university:
        join_university = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        filter_university = 'AND i.name = %(university)s'
        params['university'] = university

    filter_program = str()
    join_program = str()
    if graduate_program_id:
        params['program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON r.id = gpr.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('bp.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis
        filter_qualis = 'AND bpa.qualis = %(qualis)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_dep = str()
    join_dep = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_dep = """
            INNER JOIN ufmg.departament_researcher dr
                ON r.id = dr.researcher_id
            """
        filter_dep = 'AND dr.dep_id = %(dep_id)s'

    SCRIPT_SQL = f"""
        SELECT DISTINCT
            b.id AS id, title, b.year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors,
            opa.authors_institution, opa.citations_count, bpa.issn, opa.keywords,
            opa.landing_page_url, opa.language, opa.pdf, b.has_image, b.relevance
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_program}
            {join_dep}
            {join_university}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_pagination}
            {filter_program}
            {filter_university}
            {filter_dep}
        ORDER BY
            year DESC
        """
    result = conn.select(SCRIPT_SQL, params)
    return result
