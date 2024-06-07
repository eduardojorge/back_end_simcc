import Dao.sgbdSQL as db
import pandas as pd

from Model.Researcher import Researcher
from numpy import NaN


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
                UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
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
                r.name AS name,
                i.name AS institution,
                rp.articles AS articles,
                rp.book_chapters AS book_chapters,
                rp.book AS book,
                r.lattes_id AS lattes,
                r.lattes_10_id AS lattes_10_id,
                r.abstract AS abstract,
                UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
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
                "name",
                "university",
                "articles",
                "book_chapters",
                "book",
                "lattes_id",
                "lattes_10_id",
                "abstract",
                "area",
                "city",
                "image_university",
                "orcid",
                "graduation",
                "patent",
                "software",
                "brand",
                "lattes_update",
            ],
        )

        return data_frame.fillna(0).to_dict(orient="records")


def researcher_data_geral(year_):
    year = list(range(int(year_), 2025))

    data_frame = pd.DataFrame(year, columns=["year"])

    script_sql = f"""
        SELECT 
            g.year,
            COUNT(*) as count_guidance
        FROM
            guidance g
        WHERE
            g.year > {year_}
        GROUP BY
            g.year
        ORDER BY
            g.year;
        """

    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_guidance"])

    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_guidance"] = NaN

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK'
            AND bp.year::smallint > {year_}
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """

    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book"] = NaN

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book_chapter
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK_CHAPTER'
            AND bp.year::smallint > {year_}
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """
    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book_chapter"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book_chapter"] = NaN

    script_sql = f"""
        SELECT
            development_year,
            COUNT(DISTINCT title) as count_patent
        FROM
            patent p
        WHERE
            development_year::smallint > {year_}
        GROUP BY
            development_year
        """

    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_patent"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_patent"] = NaN

    script_sql = f"""
        SELECT
            sw.year,
            COUNT(DISTINCT title) as count_software
        FROM
            public.software sw
        WHERE
            sw.year::smallint > {year_}
        GROUP BY
            sw.year
        """

    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_software"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_software"] = NaN

    script_sql = f"""
        SELECT
            rr.year,
            COUNT(DISTINCT title) as count_report
        FROM
            research_report rr
        WHERE 
            rr.year::smallint > {year_}
        GROUP BY
            rr.year
        ORDER BY
            rr.year
        """

    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_report"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_report"] = NaN

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_article
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'ARTICLE'
            AND bp.year::smallint > {year_}
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
    """
    registre = db.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_article"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_article"] = NaN

    return data_frame.fillna(0).to_dict(orient="records")
