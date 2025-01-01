from simcc.repositories import conn
from simcc.schemas.Article import ArticleMetric


def list_article_metrics() -> list[ArticleMetric]:
    SCRIPT_SQL = """
        SELECT bp.id, bpa.jcr, year, opa.citations_count AS citations, qualis
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
        """

    result = conn.select(SCRIPT_SQL)
    return result
