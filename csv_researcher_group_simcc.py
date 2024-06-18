import sys
import pandas as pd
import project
from Dao import sgbdSQL
from Dao.resarcher_baremaSQL import researcher_production_db
from Model.Year_Barema import Year_Barema


def set_barema():
    qualis_barema = {
        "article_A1": 1.000,
        "article_A2": 0.875,
        "article_A3": 0.750,
        "article_A4": 0.625,
        "article_B1": 0.500,
        "article_B2": 0.370,
        "article_B3": 0.250,
        "article_B4": 0.125,
    }

    if int(input("Modificar os pesos para o indice de produção? [1 - Sim/0 - Não]")):
        for key, value in qualis_barema.items():
            qualis_barema[key] = int(
                input(f"Alterando o peso para produções({key}).\nDe {value} para: ")
            )

    return qualis_barema


def get_doc(lattes_id: str) -> str:

    script_sql = f"""
        SELECT
            MIN(e.education_end) as menor_education_end
        FROM
            education e
        JOIN researcher r
        ON r.id = e.researcher_id
        WHERE
            r.lattes_id = '{lattes_id}'
            AND e.degree = 'DOUTORADO';
        """
    registry = sgbdSQL.consultar_db(script_sql)

    return str(registry[0][0])


def get_researchers():
    script_sql = """
    SELECT id, name, lattes_id FROM researcher;
    """
    registry = sgbdSQL.consultar_db(script_sql)

    return pd.DataFrame(registry, columns=["id", "name", "lattes_id"])


def get_institution(lattes_id: str) -> str:
    script_sql = f"""
    SELECT
        i.acronym
    FROM
        researcher r
    JOIN institution i
    ON i.id = r.institution_id
    WHERE
    r.lattes_id = '{lattes_id}';
    """

    registry = sgbdSQL.consultar_db(script_sql)

    return pd.DataFrame(registry, columns=["acronym"])["acronym"][0]


def set_year(year: Year_Barema):

    year_input = str(input("Procurar produções(artigos) a partir do ano: "))

    year.article = year_input
    year.book = year_input
    year.chapter_book = year_input
    year.patent = year_input

    year_input = str(
        input(
            "Procurar demais produções(softwares, livros, patentes etc) a partir do ano: "
        )
    )

    year.software = year_input
    year.work_event = year_input
    year.brand = year_input
    year.resource_progress = year_input
    year.resource_completed = year_input
    year.participation_events = year_input


if __name__ == "__main__":

    year_setup = Year_Barema()
    set_year(year_setup)

    data_frame_researchers = get_researchers()

    qualis_barema = set_barema()

    lista = list()
    for Index, Data in data_frame_researchers.iterrows():
        print(Index, end=" ")
        json_barema = researcher_production_db("", Data["lattes_id"], year_setup)[0]  # fmt: skip
        json_barema["id"] = Data["id"]
        json_barema["name"] = Data["name"]

        json_barema["first_doc"] = get_doc(Data["lattes_id"])

        json_barema["institution"] = get_institution(Data["lattes_id"])
        ind_prod = 0
        for key, value in qualis_barema.items():
            if json_barema[key]:
                ind_prod += qualis_barema[key] * json_barema[key]

        json_barema["ind_prod"] = ind_prod

        lista.append(json_barema)

    data_frame_dados = pd.DataFrame(lista)

    data_frame_dados = data_frame_dados.drop(
        columns=[
            "researcher",
            "lattes_10_id",
            "event_organization",
        ]
    )
    script_sql = """
        SELECT
            bp.researcher_id as id,
            MIN(CASE WHEN bp.type = 'BOOK' THEN bp.year END) AS min_book_year,
            MIN(CASE WHEN bp.type = 'BOOK_CHAPTER' THEN bp.year END) AS min_book_chapter_year,
            MIN(CASE WHEN bp.type = 'ARTICLE' THEN bp.year END) AS min_article_year,
            MIN(CASE WHEN bp.type = 'WORK_IN_EVENT' THEN bp.year END) AS min_work_in_event_year,
            MIN(CASE WHEN bp.type = 'TEXT_IN_NEWSPAPER_MAGAZINE' THEN bp.year END) AS min_text_in_newspaper_magazine_year,
            MIN(p.development_year) as min_patent,
            MIN(g.year) as min_guidance,
            MIN(s.year) as min_software
        FROM 
            bibliographic_production bp
            LEFT JOIN researcher r ON r.id = bp.researcher_id
            LEFT JOIN patent p ON r.id = p.researcher_id
            LEFT JOIN guidance g ON r.id = g.researcher_id
            LEFT JOIN software s ON r.id = s.researcher_id
            LEFT JOIN brand b ON r.id = b.researcher_id
        GROUP BY 
            bp.researcher_id
        """
    registry = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        registry,
        columns=[
            "id",
            "min_book_year",
            "min_book_chapter_year",
            "min_article_year",
            "min_work_in_event_year",
            "min_text_in_newspaper_magazine_year",
            "min_patent_year",
            "min_guidance_year",
            "min_software",
        ],
    )
    data_frame_dados = pd.merge(data_frame_dados, df, on="id", how="left")
    data_frame_dados.to_csv("Files/researcher_group.csv")
