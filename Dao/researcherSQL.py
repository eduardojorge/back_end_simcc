import Dao.sgbdSQL as db
import pandas as pd

from Model.Researcher import Researcher


def city_search(city_name: str) -> str:
    sql = """
        SELECT id FROM city WHERE LOWER(unaccent(name)) = LOWER(unaccent('{filter}'));
        """.format(
        filter=city_name
    )
    return pd.DataFrame(db.consultar_db(sql=sql), columns=["id"])["id"][0]


def researcher_search_city(city_id):
    sql = """
        SELECT DISTINCT
            r.id AS id,
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
        LEFT JOIN
            graduate_program_researcher gpr ON r.id = gpr.researcher_id
        JOIN
            city c ON c.id = r.city_id
        JOIN
            institution i ON r.institution_id = i.id
        JOIN
            researcher_production rp ON rp.researcher_id = r.id
        WHERE
            r.city_id = '{filter}';
        """.format(
        filter=city_id
    )

    return pd.DataFrame(
        db.consultar_db(sql=sql),
        columns=[
            "id",
            "researcher_name",
            "institution",
            "article",
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
