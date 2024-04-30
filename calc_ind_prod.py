import pandas as pd

import project as project
from Dao import sgbdSQL

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
    "BOOK": 1,
    "BOOK_CHAPTER": 0.25,
}

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
    df_ind_prod_base_article = df_ind_prod_base_article.groupby("year")[
        "ind_prod_article"
    ].sum()

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

    df_ind_prod = df_ind_prod_base_book.merge(
        df_ind_prod_base_book_chapter, on="year", how="outer"
    ).merge(pd.DataFrame(df_ind_prod_base_article), on="year", how="outer")

    for Index, Data_Prod in df_ind_prod.fillna(0).iterrows():
        script_sql = f"""
        INSERT INTO public.ind_prod(
            researcher_id, year, ind_prod_article, ind_prod_book, ind_prod_book_chapter)
        VALUES ('{Data['id']}', {Data_Prod['year']}, {Data_Prod['ind_prod_article']}, {Data_Prod['ind_prod_book']}, {Data_Prod['ind_prod_book_chapter']});
        """

        sgbdSQL.execScricpt_db(script_sql)
