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
    SELECT name, lattes_id FROM researcher;
    """

    registry = sgbdSQL.consultar_db(script_sql)

    return pd.DataFrame(registry, columns=["name", "lattes_id"])


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

    year_input = str(
        input(
            "Procurar demais produções(softwares, livros, patentes etc) a partir do ano: "
        )
    )

    year.software = year_input
    year.work_event = year_input
    year.book = year_input
    year.chapter_book = year_input
    year.patent = year_input
    year.brand = year_input
    year.resource_progress = year_input
    year.resource_completed = year_input
    year.participation_events = year_input


if __name__ == "__main__":

    try:
        project.project_env = sys.argv[1]
    except:
        project.project_env = str(input("Código do banco que sera utilizado [1-8]: "))

    year_setup = Year_Barema()
    set_year(year_setup)

    data_frame_researchers = get_researchers()

    qualis_barema = set_barema()

    lista = list()
    for Index, Data in data_frame_researchers.iterrows():

        json_barema = researcher_production_db(
            "",
            Data["lattes_id"],
            year_setup,
        )[0]

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
            "book_chapter",
            "work_in_event",
            "researcher",
            "lattes_10_id",
            "event_organization",
            "participation_event",
        ]
    )

    data_frame_dados.to_csv("Files/researcher_group.csv")
