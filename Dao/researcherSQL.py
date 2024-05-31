import Dao.sgbdSQL as db
import pandas as pd

from Model.Researcher import Researcher


def city_search(city_name: str = None) -> str:
    if city_name == None:
        return None
    sql = """
        SELECT id FROM city WHERE LOWER(unaccent(name)) = LOWER(unaccent('{filter}'));
        """.format(
        filter=city_name
    )
    return pd.DataFrame(db.consultar_db(sql=sql), columns=["id"])["id"][0]


def researcher_search_city(city_id: str = None):
    if city_id == None:
        script_sql = """
            SELECT DISTINCT
                r.id AS researcher_id,
                r.name AS researcher_name,
                i.name AS institution,
                i.image AS image,
                c.name AS city,
                rp.great_area AS area
            FROM
                researcher r
            LEFT JOIN
                graduate_program_researcher gpr ON r.id = gpr.researcher_id
            JOIN
                city c ON c.id = r.city_id
            JOIN
                institution i ON r.institution_id = i.id
            JOIN
                researcher_production rp ON rp.researcher_id = r.id
            """

        registry = db.consultar_db(sql=script_sql)
        data_frame = pd.DataFrame(
            registry,
            columns=[
                "id",
                "researcher_name",
                "institution",
                "image",
                "city",
                "area",
            ],
        )
        return data_frame

    else:
        script_sql = f"""
            SELECT 
                DISTINCT r.id AS id,
                opr.h_index,
                opr.relevance_score,
                opr.works_count,
                opr.cited_by_count,
                opr.i10_index,
                opr.scopus,
                opr.openalex,
                r.name AS researcher_name,
                i.name AS institution,
                rp.articles AS articles,
                rp.book_chapters AS book_chapters,
                rp.book AS book,
                r.lattes_id AS lattes,
                r.lattes_10_id AS lattes_10_id,
                r.abstract AS abstract,
                rp.great_area AS area,
                c.name AS city,
                i.image AS image,
                r.orcid AS orcid,
                r.graduation AS graduation,
                rp.patent AS patent,
                rp.software AS software,
                rp.brand AS brand,
                TO_CHAR(r.last_update, 'dd/mm/yyyy') AS lattes_update
            FROM
                researcher r
                LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
                LEFT JOIN city c ON c.id = r.city_id
                LEFT JOIN institution i ON r.institution_id = i.id
                LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
                LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
            WHERE
                r.city_id = '{city_id}';
            """
        registry = db.consultar_db(script_sql)

        data_frame = pd.DataFrame(
            registry,
            columns=[
                "id",
                "h_index",
                "relevance_score",
                "works_count",
                "cited_by_count",
                "i10_index",
                "scopus",
                "openalex",
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
                "image",
                "orcid",
                "graduation",
                "patent",
                "software",
                "brand",
                "lattes_update",
            ],
        )

        return data_frame.fillna(0).to_dict(orient="records")
