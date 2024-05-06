import pandas as pd
from numpy import NaN

import project as project
from Dao import sgbdSQL


def article_prod(Data):

    script_sql = f"""
        SELECT
            year,
            qualis,
            COUNT(*) AS count_article
        FROM
            public.bibliographic_production bp
        JOIN
            public.bibliographic_production_article bpa ON
            bp.id = bpa.bibliographic_production_id
        WHERE
            researcher_id = '{Data['id']}'
            AND type = 'ARTICLE'
        GROUP BY
            year, qualis
        ORDER BY
            year, qualis;
        """
    registry = sgbdSQL.consultar_db(script_sql)

    df_ind_prod_base_article = pd.DataFrame(
        registry, columns=["year", "qualis", "count_article"]
    )
    df_ind_prod_base_article["ind_prod_article"] = (
        df_ind_prod_base_article["qualis"].map(weights)
        * df_ind_prod_base_article["count_article"]
    )

    df_ind_prod_base_article = df_ind_prod_base_article.groupby("year", as_index=False)[
        "ind_prod_article"
    ].sum()
    df_ind_prod_base_article["year"] = df_ind_prod_base_article["year"].astype(int)
    return df_ind_prod_base_article


def book_prod(Data):
    script_sql = f"""
        SELECT
            year,
            COUNT(*) AS count_book
        FROM
            public.bibliographic_production bp
        WHERE
            researcher_id = '{Data['id']}'
            AND type = 'BOOK'
        GROUP BY
            year
        ORDER BY
            year;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    df_ind_prod_base_book = pd.DataFrame(registry, columns=["year", "count_book"])

    df_ind_prod_base_book["ind_prod_book"] = (
        df_ind_prod_base_book["count_book"] * weights["BOOK"]
    )
    df_ind_prod_base_book = df_ind_prod_base_book.drop("count_book", axis=1)
    df_ind_prod_base_book["year"] = df_ind_prod_base_book["year"].astype(int)
    return df_ind_prod_base_book


def book_chapter_prod(Data):

    script_sql = f"""
        SELECT
            year,
            COUNT(*) AS count_book_chapter
        FROM
            public.bibliographic_production bp
        WHERE
            researcher_id = '{Data['id']}'
            AND type = 'BOOK_CHAPTER'
        GROUP BY
            year
        ORDER BY
            year;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    df_ind_prod_base_book_chapter = pd.DataFrame(
        registry, columns=["year", "count_book_chapter"]
    )

    df_ind_prod_base_book_chapter["ind_prod_book_chapter"] = (
        df_ind_prod_base_book_chapter["count_book_chapter"] * weights["BOOK_CHAPTER"]
    )
    df_ind_prod_base_book_chapter = df_ind_prod_base_book_chapter.drop(
        "count_book_chapter", axis=1
    )
    df_ind_prod_base_book_chapter["year"] = df_ind_prod_base_book_chapter[
        "year"
    ].astype(int)
    return df_ind_prod_base_book_chapter


def patent_prod(Data):
    script_sql = f"""
        SELECT
            development_year,
            'PATENT_GRANTED' as granted,
            COUNT(*) as count_patent
        FROM 
            patent p
        WHERE
            p.researcher_id = '{Data['id']}'
            AND grant_date IS NOT NULL
        GROUP BY 
            development_year

        UNION

        SELECT
            development_year,
            'PATENT_NOT_GRANTED',
            COUNT(*) as count_patent
        FROM 
            patent p
        WHERE
            p.researcher_id = '{Data['id']}'
            AND grant_date IS NULL
        GROUP BY 
            development_year
        """
    registry = sgbdSQL.consultar_db(script_sql)

    df_ind_prod_base_patent = pd.DataFrame(
        registry, columns=["year", "granted", "count_patent"]
    )

    df_ind_prod_base_patent["ind_prod_patent"] = (
        df_ind_prod_base_patent["granted"].map(weights)
        * df_ind_prod_base_patent["count_patent"]
    )
    df_ind_prod_base_patent = df_ind_prod_base_patent.pivot(
        index="year", columns="granted", values="ind_prod_patent"
    )
    df_ind_prod_base_patent = df_ind_prod_base_patent.rename(
        columns={
            "PATENT_GRANTED": "ind_prod_granted_patent",
            "PATENT_NOT_GRANTED": "ind_prod_not_granted_patent",
        }
    )
    df_ind_prod_base_patent = df_ind_prod_base_patent.reset_index()
    df_ind_prod_base_patent["year"] = df_ind_prod_base_patent["year"].astype(int)

    return df_ind_prod_base_patent


def software_prod(Data):
    script_sql = f"""
        SELECT
            year,
            COUNT(*) as count_software
        FROM 
            public.software
        WHERE
            researcher_id = '{Data['id']}'
        GROUP BY
            researcher_id, year
        """

    registry = sgbdSQL.consultar_db(script_sql)

    df_ind_prod_base_software = pd.DataFrame(
        registry, columns=["year", "count_software"]
    )

    df_ind_prod_base_software["ind_prod_software"] = (
        df_ind_prod_base_software["count_software"] * weights["SOFTWARE"]
    )
    df_ind_prod_base_software["year"] = df_ind_prod_base_software["year"].astype(int)
    return df_ind_prod_base_software


def report_prod(Data):
    script_sql = f"""
        SELECT
            year,
            COUNT(*) as count_report
        FROM 
            research_report 
        WHERE 
            researcher_id = '{Data['id']}'
        GROUP BY
            year
        ORDER BY 
            year
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=["year", "count_report"],
    )

    data_frame["ind_prod_report"] = data_frame["count_report"] * weights["REPORT"]

    return data_frame


if __name__ == "__main__":

    project.project_env = "4"

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
    }

    year = list(range(2008, 2025))

    script_sql = """
        SELECT 
            id, 
            name 
        FROM 
            researcher
        """

    registry = sgbdSQL.consultar_db(script_sql)

    df_researchers = pd.DataFrame(registry, columns=["id", "name"])

    for Index, Data in df_researchers.iterrows():

        data_frame = pd.DataFrame(year, columns=["year"])

        df = article_prod(Data=Data)

        if not df.empty:
            data_frame = pd.merge(
                data_frame, article_prod(Data=Data), on="year", how="left"
            )
        else:
            data_frame["ind_prod_article"] = NaN

        df = book_prod(Data=Data)

        if not df.empty:
            data_frame = pd.merge(data_frame, df, on="year", how="left")
        else:
            data_frame["ind_prod_book"] = NaN

        df = book_chapter_prod(Data=Data)

        if not df.empty:
            data_frame = pd.merge(data_frame, df, on="year", how="left")
        else:
            data_frame["ind_prod_book_chapter"] = NaN

        df = patent_prod(Data=Data)
        if not df.empty:
            data_frame = pd.merge(data_frame, df, on="year", how="left")
        if "ind_prod_granted_patent" not in df.columns:
            data_frame["ind_prod_granted_patent"] = NaN
        if "ind_prod_not_granted_patent" not in df.columns:
            data_frame["ind_prod_not_granted_patent"] = NaN

        df = software_prod(Data=Data)

        if not df.empty:
            data_frame = pd.merge(data_frame, df, on="year", how="left")
        else:
            data_frame["ind_prod_software"] = NaN

        df = report_prod(Data=Data)

        if not df.empty:
            data_frame = pd.merge(data_frame, df, on="year", how="left")
        else:
            data_frame["ind_prod_report"] = NaN

        for Intern_Index, Intern_Data in data_frame.fillna(0).iterrows():
            script_sql = f"""
            INSERT INTO public.researcher_ind_prod(
            researcher_id, year, ind_prod_article, ind_prod_book, ind_prod_book_chapter, ind_prod_software, ind_prod_granted_patent, ind_prod_not_granted_patent, ind_prod_report)
            VALUES ('{Data['id']}', {Intern_Data['year']}, {Intern_Data['ind_prod_article']}, {Intern_Data['ind_prod_book']}, {Intern_Data['ind_prod_book_chapter']}, {Intern_Data['ind_prod_software']}, {Intern_Data['ind_prod_granted_patent']}, {Intern_Data['ind_prod_not_granted_patent']}, {Intern_Data['ind_prod_report']});
            """
            sgbdSQL.execScript_db(script_sql)