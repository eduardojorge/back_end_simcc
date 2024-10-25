import datetime
from dotenv import load_dotenv
import pandas as pd
from Dao import sgbdSQL


def article_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            qualis,
            COUNT(*) AS count_article,
            researcher_id
        FROM
            public.bibliographic_production bp
        JOIN
            public.bibliographic_production_article bpa ON
            bp.id = bpa.bibliographic_production_id
        WHERE
            type = 'ARTICLE'
        GROUP BY
            year, qualis, researcher_id;
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry, columns=["year", "qualis", "count_article", "researcher_id"]
    )
    data_frame = data_frame.pivot_table(
        index=["year", "researcher_id"],
        columns="qualis",
        values="count_article",
        aggfunc="sum",
        fill_value=0,
    )
    data_frame.reset_index(inplace=True)

    SCRIPT_SQL = """
        SELECT
            bp.researcher_id,
            bp.year,
            COUNT(bpa.jcr)
        FROM bibliographic_production bp
        INNER JOIN bibliographic_production_article bpa ON 
            bpa.bibliographic_production_id = bp.id 
            AND bpa.jcr IS NOT NULL
        GROUP BY
            researcher_id, year
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    df = pd.DataFrame(registry, columns=["researcher_id", "year", "JCR"])

    data_frame = pd.merge(data_frame, df, on=["researcher_id", "year"], how="left")

    data_frame["year"] = data_frame["year"].astype(int)

    return data_frame


def book_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) AS count_book,
            researcher_id
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK'
        GROUP BY
            year, researcher_id;
        """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    df_ind_prod_base_book = pd.DataFrame(
        registry, columns=["year", "BOOK", "researcher_id"]
    )
    df_ind_prod_base_book["year"] = df_ind_prod_base_book["year"].astype(int)
    return df_ind_prod_base_book


def book_chapter_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) AS count_book_chapter,
            researcher_id
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK_CHAPTER'
        GROUP BY
            year, researcher_id;
        """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    df_ind_prod_base_book_chapter = pd.DataFrame(
        registry, columns=["year", "BOOK_CHAPTER", "researcher_id"]
    )

    df_ind_prod_base_book_chapter["year"] = df_ind_prod_base_book_chapter[
        "year"
    ].astype(int)
    return df_ind_prod_base_book_chapter


def patent_prod():
    SCRIPT_SQL = """
        SELECT
            development_year,
            CASE
                WHEN grant_date IS NOT NULL THEN 'PATENT_GRANTED'
                ELSE 'PATENT_NOT_GRANTED'
            END AS granted,
            COUNT(*) AS count_patent,
            researcher_id
        FROM
            patent p
        GROUP BY
            development_year,
            researcher_id,
            grant_date
    """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    df_ind_prod_base_patent = pd.DataFrame(
        registry, columns=["year", "granted", "count_patent", "researcher_id"]
    )

    df_ind_prod_base_patent["year"] = df_ind_prod_base_patent["year"].astype(int)

    df_pivot = df_ind_prod_base_patent.pivot_table(
        index=["year", "researcher_id"],
        columns="granted",
        values="count_patent",
        fill_value=0,
    ).reset_index()

    if "PATENT_GRANTED" not in df_pivot.columns:
        df_pivot["PATENT_GRANTED"] = 0

    if "PATENT_NOT_GRANTED" not in df_pivot.columns:
        df_pivot["PATENT_NOT_GRANTED"] = 0

    return df_pivot


def software_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*),
            researcher_id
        FROM
            public.software
        GROUP BY
            researcher_id, year
        """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    df_ind_prod_base_software = pd.DataFrame(
        registry, columns=["year", "SOFTWARE", "researcher_id"]
    )

    df_ind_prod_base_software["year"] = df_ind_prod_base_software["year"].astype(int)
    return df_ind_prod_base_software


def report_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) as count_report,
            researcher_id
        FROM
            research_report
        GROUP BY
            year, researcher_id;
        """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=["year", "REPORT", "researcher_id"],
    )
    return data_frame


def guidance_prod():
    SCRIPT_SQL = """
        SELECT
            g.year,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature_status,
            COUNT(*) as count_nature,
            researcher_id
        FROM
            guidance g
        GROUP BY
            g.year, nature_status, researcher_id;
    """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame_guidance = pd.DataFrame(
        registry,
        columns=["year", "nature", "count_nature", "researcher_id"],
    )

    data_frame_guidance = data_frame_guidance.pivot_table(
        index=["year", "researcher_id"],
        columns="nature",
        values="count_nature",
        fill_value=0,
    ).reset_index()

    rename_dict = {
        "iniciacao cientifica concluida": "GUIDANCE_IC_C",
        "iniciacao cientifica em andamento": "GUIDANCE_IC_A",
        "dissertacao de mestrado concluida": "GUIDANCE_M_C",
        "dissertacao de mestrado em andamento": "GUIDANCE_M_A",
        "tese de doutorado concluida": "GUIDANCE_D_C",
        "tese de doutorado em andamento": "GUIDANCE_D_A",
        "trabalho de conclusao de curso graduacao concluida": "GUIDANCE_G_C",
        "trabalho de conclusao de curso de graduacao em andamento": "GUIDANCE_G_A",
        "monografia de conclusao de curso aperfeicoamento e especializacao concluida": "GUIDANCE_E_C",
        "monografia de conclusao de curso aperfeicoamento e especializacao em andamento": "GUIDANCE_E_A",
        "orientacao-de-outra-natureza concluida": "GUIDANCE_O_C",
        "supervisao de pos-doutorado concluida": "GUIDANCE_SD_C",
        "supervisao de pos-doutorado em andamento": "GUIDANCE_SD_A",
    }

    data_frame_guidance.rename(columns=rename_dict, inplace=True)

    data_frame_guidance = data_frame_guidance.reindex(
        columns=["year", "researcher_id"] + list(rename_dict.values()), fill_value=0
    )
    return data_frame_guidance


def brand_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) as count_brand,
            researcher_id
        FROM
            brand
        GROUP BY
            year, researcher_id
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame_brand = pd.DataFrame(
        registry,
        columns=["year", "BRAND", "researcher_id"],
    )
    return data_frame_brand


def work_in_event_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) AS count_we,
            researcher_id
        FROM
            bibliographic_production
        WHERE
            type = 'WORK_IN_EVENT'
        GROUP BY
            year, researcher_id
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=["year", "WORK_IN_EVENT", "researcher_id"],
    )
    data_frame["year"] = data_frame["year"].astype(int)
    return data_frame


def research_project_prod():
    SCRIPT_SQL = """
        SELECT 
            researcher_id,
            end_year,
            COUNT(*)
        FROM 
            research_project
        GROUP BY 
            researcher_id, end_year;
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry, columns=["researcher_id", "year", "RESEARCH_PROJECT"]
    )

    return data_frame


def event_organization_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*) as count_event_organization,
            researcher_id
        FROM
            event_organization
        GROUP BY
            year, researcher_id
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=["year", "EVENT_ORGANIZATION", "researcher_id"],
    )
    data_frame["year"] = data_frame["year"].astype(int)
    return data_frame


def participation_event_prod():
    SCRIPT_SQL = """
        SELECT
            year,
            COUNT(*),
            researcher_id
        FROM
            participation_events
        GROUP BY
            year, researcher_id
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=["year", "PARTICIPATION_EVENTS", "researcher_id"],
    )
    return data_frame


def researcher_data():
    SCRIPT_SQL = """
        SELECT
            researcher.id, researcher.name, researcher.graduation, researcher.lattes_id, education.education_end, i.acronym,
            bp_book.year, bp_chapter.year, bp_article.year, bp_work.year, bp_text.year, p.development_year,
            g.year, s.year
        FROM researcher
        LEFT JOIN institution i ON researcher.institution_id = i.id
        LEFT JOIN (
            SELECT researcher_id, MAX(education_end) AS education_end
            FROM education
            WHERE degree = 'DOUTORADO'
            GROUP BY researcher_id
        ) education ON researcher.id = education.researcher_id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM bibliographic_production
            WHERE type = 'BOOK'
            GROUP BY researcher_id
        ) bp_book ON bp_book.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM bibliographic_production
            WHERE type = 'BOOK_CHAPTER'
            GROUP BY researcher_id
        ) bp_chapter ON bp_chapter.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM bibliographic_production
            WHERE type = 'ARTICLE'
            GROUP BY researcher_id
        ) bp_article ON bp_article.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM bibliographic_production
            WHERE type = 'WORK_IN_EVENT'
            GROUP BY researcher_id
        ) bp_work ON bp_work.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM bibliographic_production
            WHERE type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
            GROUP BY researcher_id
        ) bp_text ON bp_text.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(development_year) AS development_year
            FROM patent
            GROUP BY researcher_id
        ) p ON p.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM guidance
            GROUP BY researcher_id
        ) g ON g.researcher_id = researcher.id
        LEFT JOIN (
            SELECT researcher_id, MIN(YEAR) AS YEAR
            FROM software
            GROUP BY researcher_id
        ) s ON s.researcher_id = researcher.id
        GROUP BY researcher.id, researcher.name, researcher.graduation, education.education_end, i.acronym,
            bp_book.year, bp_chapter.year, bp_article.year, bp_work.year, bp_text.year, p.development_year, g.year, s.year;
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "researcher_id",
            "NAME",
            "GRADUATION",
            "LATTES_ID",
            "FIRST_DOC",
            "INSTITUTION",
            "MIN_BOOK",
            "MIN_BOOK_CHAPTER",
            "MIN_ARTICLE",
            "MIN_WORK_IN_EVENT",
            "MIN_TEXT_IN_NEWSPAPER_MAGAZINE",
            "MIN_PATENT",
            "MIN_GUIDANCE",
            "MIN_SOFTWARE",
        ],
    )
    return data_frame


def class_researcher(researcher):
    year = datetime.datetime.now().year
    GUIDANCE_DOC = researcher["GUIDANCE_D_C"] + researcher["GUIDANCE_D_A"]
    GUIDANCE_MAS = researcher["GUIDANCE_M_C"] + researcher["GUIDANCE_M_A"]
    QUALIS_A = researcher["A1"] + researcher["A2"] + researcher["A3"] + researcher["A4"]
    QUALIS_B = researcher["B1"] + researcher["B2"] + researcher["B3"] + researcher["B4"]

    if year - researcher["FIRST_DOC"] >= 10:
        if (researcher["A1"] >= 2 and GUIDANCE_DOC >= 4) or (
            researcher["A1"] >= 1 and researcher["PATENT_GRANTED"] >= 1
        ):
            return "A+"
        if (researcher["A1"] >= 1 and GUIDANCE_DOC >= 2) or (
            researcher["PATENT_GRANTED"] >= 1
        ):
            return "A"

    if year - researcher["FIRST_DOC"] >= 8:
        if (QUALIS_A >= 2 and (GUIDANCE_MAS >= 2 or GUIDANCE_DOC >= 1)) or (
            QUALIS_A >= 1
            and (researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3)
        ):
            return "B+"
        if (QUALIS_A >= 1 and (GUIDANCE_MAS >= 2 or GUIDANCE_DOC >= 1)) or (
            researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3
        ):
            return "B"

    if year - researcher["FIRST_DOC"] >= 6:
        if (QUALIS_A >= 2 and (GUIDANCE_MAS >= 1 or GUIDANCE_DOC >= 1)) or (
            QUALIS_A >= 1
            and (researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3)
        ):
            return "C+"
        if (QUALIS_A >= 1 and (GUIDANCE_MAS >= 1 or GUIDANCE_DOC >= 1)) or (
            researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3
        ):
            return "C"

    if year - researcher["FIRST_DOC"] >= 3:
        if QUALIS_A >= 1 or (
            researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3
        ):
            return "D"
        if QUALIS_B >= 1 or (
            researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3
        ):
            return "D+"

    if year - researcher["FIRST_DOC"] > 0:
        if QUALIS_B >= 1 or (
            QUALIS_A >= 1
            and (researcher["PATENT_GRANTED"] >= 1 or researcher["SOFTWARE"] >= 3)
        ):
            return "E+"
    return "E"


def education_prod():
    SCRIPT_SQL = """
        SELECT 
            researcher_id, 
            degree, 
            COUNT(*) AS total  
        FROM 
            education 
        WHERE
            education_end IS NOT NULL
        GROUP BY 
            researcher_id, 
            degree
        ORDER BY 
            researcher_id;
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

    data_frame = pd.DataFrame(registry, columns=["researcher_id", "degree", "count"])
    data_frame = data_frame.pivot_table(
        index=["researcher_id"],
        columns="degree",
        values="count",
        fill_value=0,
    ).reset_index()
    data_frame = data_frame.reindex(
        columns=["researcher_id"]
        + [
            "DOUTORADO",
            "ESPECIALIZACAO",
            "MESTRADO",
            "GRADUACAO",
            "MESTRADO-PROFISSIONALIZANTE",
        ],
        fill_value=0,
    )
    return data_frame


def apply_barema(data_frame):
    print(data_frame.columns)
    data_frame["BAREMA_MESTRADO"] = data_frame.apply(
        lambda row: 1.5
        if row["MESTRADO"] > 0 or row["MESTRADO-PROFISSIONALIZANTE"] > 0
        else 0,
        axis=1,
    )
    data_frame["BAREMA_ESPECIALIZACAO"] = (
        data_frame["ESPECIALIZACAO"].clip(upper=2) * 0.25
    )

    data_frame["BAREMA_RESEARCH_PROJECT"] = (
        data_frame["RESEARCH_PROJECT"].clip(upper=2) * 0.25
    )

    INDEXED = ["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C", "JCR"]
    data_frame["BAREMA_INDEXED_ARTICLE"] = data_frame[INDEXED].sum(axis=1)

    data_frame["BAREMA_INDEXED_ARTICLE"] = (
        data_frame["BAREMA_INDEXED_ARTICLE"].clip(upper=3) * 0.5
    )

    data_frame["BAREMA_NOT_INDEXED_ARTICLE"] = data_frame["SQ"].clip(upper=2) * 0.25

    data_frame["BAREMA_BOOK"] = data_frame["BOOK"].clip(upper=3) * 0.5

    data_frame["BAREMA_BOOK_CHAPTER"] = data_frame["BOOK_CHAPTER"].clip(upper=4) * 0.25

    THECHNICAL_PRODUCTION = [
        "SOFTWARE",
        "PATENT_NOT_GRANTED",
        "PATENT_GRANTED",
        "BRAND",
    ]
    data_frame["BAREMA_THECHNICAL_PRODUCTION"] = data_frame[THECHNICAL_PRODUCTION].sum(
        axis=1
    )
    data_frame["BAREMA_THECHNICAL_PRODUCTION"] = (
        data_frame["BAREMA_THECHNICAL_PRODUCTION"].clip(upper=5) * 0.25
    )

    data_frame["BAREMA_EVENT_ORGANIZATION"] = (
        data_frame["EVENT_ORGANIZATION"].clip(upper=5) * 0.10
    )

    GUIDANCE = [
        "GUIDANCE_IC_C",
        "GUIDANCE_IC_A",
        "GUIDANCE_M_C",
        "GUIDANCE_M_A",
        "GUIDANCE_D_C",
        "GUIDANCE_D_A",
        "GUIDANCE_G_C",
        "GUIDANCE_G_A",
        "GUIDANCE_E_C",
        "GUIDANCE_E_A",
        "GUIDANCE_O_C",
        "GUIDANCE_SD_C",
        "GUIDANCE_SD_A",
    ]
    data_frame["BAREMA_GUIDANCE"] = data_frame[GUIDANCE].sum(axis=1)
    data_frame["BAREMA_GUIDANCE"] = data_frame["BAREMA_GUIDANCE"].clip(upper=5) * 0.10
    return data_frame


weights = {
    "A1": 1,
    "A2": 0.875,
    "A3": 0.75,
    "A4": 0.625,
    "B1": 0.5,
    "B2": 0.375,
    "B3": 0.25,
    "B4": 0.125,
    "C": 0,
    "SQ": 0,
    "BOOK": 1,
    "BOOK_CHAPTER": 0.25,
    "SOFTWARE": 0.25,
    "PATENT_GRANTED": 1,
    "PATENT_NOT_GRANTED": 0.25,
    "REPORT": 0.25,
    "GUIDANCE_D_C": 0.5,
    "GUIDANCE_D_A": 0.25,
    "GUIDANCE_M_C": 0.25,
    "GUIDANCE_M_A": 0.125,
    "GUIDANCE_IC_C": 0.125,
    "GUIDANCE_IC_A": 0.1,
}

if __name__ == "__main__":
    load_dotenv(override=True)

    year = list(range(2019, 2025))

    SCRIPT_SQL = "SELECT id FROM researcher"
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    researchers = pd.DataFrame(registry, columns=["researcher_id"])

    years = pd.DataFrame(year, columns=["year"])

    data_frame = pd.merge(
        researchers.assign(key=1), years.assign(key=1), on="key"
    ).drop("key", axis=1)

    data_frame = pd.merge(
        data_frame, article_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, book_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, book_chapter_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, patent_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, software_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, report_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, guidance_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, brand_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, work_in_event_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, event_organization_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, participation_event_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, research_project_prod(), on=["year", "researcher_id"], how="left"
    )

    data_frame.fillna(0, inplace=True)

    data_frame = data_frame.drop(columns="year")

    data_frame = data_frame.groupby("researcher_id").sum().reset_index()

    data_frame = pd.merge(
        data_frame, education_prod(), on=["researcher_id"], how="left"
    )

    data_frame = pd.merge(
        data_frame, researcher_data(), on=["researcher_id"], how="left"
    )

    data_frame["IND_PROD"] = sum(
        data_frame[col] * weight for col, weight in weights.items()
    )

    data_frame = apply_barema(data_frame)

    df = pd.read_csv("Files/lattes_list.csv")

    df["LATTES_ID"] = df["LATTES"].apply(lambda x: str(x).split("/")[-1])

    data_frame = pd.merge(df, data_frame, how="left", on="LATTES_ID")

    data_frame.to_csv(
        "Files/researcher_group.csv", index=False, encoding="utf-8-sig", decimal=","
    )
