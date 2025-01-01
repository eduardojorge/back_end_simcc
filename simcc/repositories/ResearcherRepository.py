from simcc.repositories import conn
from simcc.schemas.Researcher import ResearcherArticleProduction


def list_article_production() -> list[ResearcherArticleProduction]:
    SCRIPT_SQL = """
        SELECT r.name, bpa.qualis, COUNT(*) AS among, bp.year,
            COALESCE(SUM(opa.citations_count), 0) AS citations
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
        GROUP BY r.id, bpa.qualis, bp.year;
        """

    result = conn.select(SCRIPT_SQL)
    return result
