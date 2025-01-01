from uuid import UUID

from simcc.repositories import conn
from simcc.schemas.Article import ArticleMetric


def list_article_metrics(program_id: UUID, year: int) -> list[ArticleMetric]:
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
        SELECT bp.id, NULLIF(bpa.jcr, 'N/A') AS jcr, bp.year,
            COALESCE(opa.citations_count, 0) AS citations, qualis
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        WHERE 1 = 1
            {program_filter}
            {year_filter};
            """

    result = conn.select(SCRIPT_SQL, params)
    return result
