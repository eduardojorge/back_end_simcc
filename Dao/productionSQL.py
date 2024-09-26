import pandas as pd
import Dao.sgbdSQL as db


def participation_events(year: int, researcher_id=None):
    SCRIPT_SQL = """
        SELECT
            title,
            title_en,
            nature,
            language,
            means_divulgation,
            homepage,
            relevance,
            scientific_divulgation,
            authors,
            year_
        FROM
            public.bibliographic_production
        WHERE
            type = 'WORK_IN_EVENT'
        """
    if researcher_id:
        SCRIPT_SQL += f"AND researcher_id = '{researcher_id}' "
    if year:
        SCRIPT_SQL += f"AND year_ >= {year} "

    SCRIPT_SQL += "ORDER BY year_ desc"

    registry = db.consultar_db(SCRIPT_SQL)
    dataframe = pd.DataFrame(
        registry,
        columns=[
            "title",
            "title_en",
            "nature",
            "language",
            "means_divulgation",
            "homepage",
            "relevance",
            "scientific_divulgation",
            "authors",
            "year_",
        ],
    )

    return dataframe.to_dict(orient="records")


def text_in_newpaper_magazine(year: int, researcher_id=None):
    SCRIPT_SQL = """
        SELECT
            title,
            title_en,
            nature,
            language,
            means_divulgation,
            homepage,
            relevance,
            scientific_divulgation,
            authors,
            year_
        FROM
            public.bibliographic_production
        WHERE
            type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
        """
    if researcher_id:
        SCRIPT_SQL += f"AND researcher_id = '{researcher_id}'"
    if year:
        SCRIPT_SQL += f"AND year_ >= {year}"
    registry = db.consultar_db(SCRIPT_SQL)
    dataframe = pd.DataFrame(
        registry,
        columns=[
            "title",
            "title_en",
            "nature",
            "language",
            "means_divulgation",
            "homepage",
            "relevance",
            "scientific_divulgation",
            "authors",
            "year_",
        ],
    )

    return dataframe.to_dict(orient="records")
