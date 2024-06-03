import pandas as pd

from Dao import sgbdSQL as bd


def search_term(term: list):
    search_string = str(" <-> ").join(term)

    filter = f"AND to_tsvector(r.abstract) @@ to_tsquery('{search_string}')"

    script_sql = f"""
        SELECT 
            r.id AS id,
            COUNT(DISTINCT b.id) AS qtd,
            r.name AS researcher_name,
            i.name AS institution,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            r.lattes_id AS lattes,
            r.lattes_10_id AS lattes_10_id,
            abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image,
            r.graduation AS graduation,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            TO_CHAR(r.last_update, 'dd/mm/yyyy') AS lattes_update,
            '{" ".join(term)}' AS terms
        FROM 
            researcher r
        LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
        JOIN institution i ON r.institution_id = i.id
        JOIN researcher_production rp ON rp.researcher_id = r.id
        JOIN city c ON c.id = r.city_id
        JOIN bibliographic_production b ON b.researcher_id = r.id
        WHERE 
            b.type = 'ARTICLE'
            {filter}
        GROUP BY 
            r.id,
            r.name,
            i.name,
            rp.articles,
            rp.book_chapters,
            rp.book,
            r.lattes_id,
            r.lattes_10_id,
            abstract,
            rp.great_area,
            rp.city,
            r.orcid,
            i.image,
            r.graduation,
            rp.patent,
            rp.software,
            rp.brand,
            TO_CHAR(r.last_update, 'dd/mm/yyyy'),
            terms
        ORDER BY 
            COUNT(DISTINCT b.id) DESC;
        """
    registry = bd.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "qtd",
            "researcher_name",
            "institution",
            "articles",
            "book_chapters",
            "book",
            "lattes",
            "lattes_10_id",
            "abstract",
            "area",
            "city",
            "orcid",
            "image",
            "graduation",
            "patent",
            "software",
            "brand",
            "lattes_update",
            "terms",
        ],
    )

    return data_frame
