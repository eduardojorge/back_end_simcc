from dotenv import load_dotenv
import unidecode
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

    df_ind_prod_base_article = pd.DataFrame(
        registry, columns=["year", "qualis", "count_article", "researcher_id"]
    )
    df_ind_prod_base_article = df_ind_prod_base_article.pivot_table(
        index=["year", "researcher_id"],
        columns="qualis",
        values="count_article",
        aggfunc="sum",
        fill_value=0,
    )
    df_ind_prod_base_article.reset_index(inplace=True)

    df_ind_prod_base_article["year"] = df_ind_prod_base_article["year"].astype(
        int)
    return df_ind_prod_base_article


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

    df_ind_prod_base_patent["year"] = df_ind_prod_base_patent["year"].astype(
        int)

    df_pivot = df_ind_prod_base_patent.pivot_table(
        index=["year", "researcher_id"],
        columns="granted",
        values="count_patent",
        fill_value=0,
    ).reset_index()

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

    df_ind_prod_base_software["year"] = df_ind_prod_base_software["year"].astype(
        int)
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

    data_frame = pd.DataFrame(registry, columns=[
        'researcher_id', 'NAME', 'GRADUATION', 'LATTES_ID', 'FIRST_DOC',
        'INSTITUTION', 'MIN_BOOK', 'MIN_BOOK_CHAPTER', 'MIN_ARTICLE',
        'MIN_WORK_IN_EVENT', 'MIN_TEXT_IN_NEWSPAPER_MAGAZINE', 'MIN_PATENT',
        'MIN_GUIDANCE', 'MIN_SOFTWARE',
    ])
    return data_frame


def classificar_pesquisador(row):
    tempo_doutorado = row['FIRST_DOC']
    A1 = row['A1']
    A2 = row['A2']
    A3 = row['A3']
    A4 = row['A4']
    B1 = row['B1']
    B2 = row['B2']
    B3 = row['B3']
    B4 = row['B4']
    C = row['C']
    SQ = row['SQ']
    patente_granted = row['PATENT_GRANTED']
    software = row['SOFTWARE']

    if tempo_doutorado >= 10 and A1 >= 2 and row['GUIDANCE_M_C'] >= 4 and (A1 >= 1 and patente_granted >= 1):
        return 'A+'
    elif tempo_doutorado >= 10 and A1 >= 1 and row['GUIDANCE_M_C'] >= 2 and patente_granted >= 1:
        return 'A'
    elif tempo_doutorado >= 8 and (A1 + A2 + A3 + A4) >= 2 and (row['GUIDANCE_M_A'] >= 2 or row['GUIDANCE_M_C'] >= 1) and (A1 + A2 + A3 + A4 >= 1 and (patente_granted >= 1 or software >= 3)):
        return 'B+'
    elif tempo_doutorado >= 8 and (A1 + A2 + A3 + A4) >= 1 and (row['GUIDANCE_M_A'] >= 2 or row['GUIDANCE_M_C'] >= 1) and (patente_granted >= 1 or software >= 3):
        return 'B'
    elif tempo_doutorado >= 6 and (A1 + A2 + A3 + A4) >= 2 and (row['GUIDANCE_IC_A'] >= 1 or row['GUIDANCE_IC_C'] >= 1) and (A1 + A2 + A3 + A4 >= 1 and (patente_granted >= 1 or software >= 3)):
        return 'C+'
    elif tempo_doutorado >= 6 and (A1 + A2 + A3 + A4) >= 1 and (row['GUIDANCE_IC_A'] >= 1 or row['GUIDANCE_IC_C'] >= 1) and (patente_granted >= 1 or software >= 3):
        return 'C'
    elif tempo_doutorado >= 3 and (A1 + A2 + A3 + A4) >= 1 and (patente_granted >= 1 or software >= 3):
        return 'D+'
    elif tempo_doutorado >= 3 and (B1 + B2 + B3 + B4) >= 1 and (patente_granted >= 1 or software >= 3):
        return 'D'
    elif tempo_doutorado < 3 and (B1 + B2 + B3 + B4) >= 1 and (A1 >= 1 or patente_granted >= 1 or software >= 3):
        return 'E+'
    else:
        return 'E'


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

    year = list(range(2008, 2025))

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

    data_frame.fillna(0, inplace=True)

    data_frame = data_frame.drop(columns='year')

    data_frame = data_frame.groupby("researcher_id").sum().reset_index()

    data_frame['IND_PROD'] = sum(
        data_frame[col] * weight for col, weight in weights.items())

    data_frame = pd.merge(
        data_frame, researcher_data(), on=["researcher_id"], how="left"
    )

    data_frame['CLASS'] = data_frame.apply(classificar_pesquisador, axis=1)

    data_frame.to_csv(
        "Files/researcher_group.csv", index=False, encoding='utf-8-sig'
    )
