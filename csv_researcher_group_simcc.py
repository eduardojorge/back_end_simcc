import sys
import project
import pandas as pd
from Dao import sgbdSQL
from Dao import resarcher_baremaSQL
from Model.Year_Barema import Year_Barema

if __name__ == "__main__":
    try:
        project.project_env = sys.argv[1]
    except:
        project.project_env = str(input("Código do banco que sera utilizado [1-8]: "))

    year_input = str(input("Procurar produções a partir do ano: "))

    year = Year_Barema()
    year.article = year_input
    year.work_event = year_input
    year.book = year_input
    year.chapter_book = year_input
    year.patent = year_input
    year.software = year_input
    year.brand = year_input
    year.resource_progress = year_input
    year.resource_completed = year_input
    year.participation_events = year_input

    script_sql = """
    SELECT id, lattes_id FROM researcher WHERE id = 'c5d92692-50e8-4562-b5ce-e3fcbaa624be';
    """

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame_lattes = pd.DataFrame(reg, columns=["id", "lattes_id"])

    lista = list()
    for Index, Data in data_frame_lattes.iterrows():

        script_sql = f"""
        SELECT
            MIN(e.education_end) as menor_education_end
        FROM
            education e
        JOIN researcher r
        ON r.id = e.researcher_id
        WHERE
            r.lattes_id = '{Data["lattes_id"]}'
            AND e.degree = 'DOUTORADO';
        """
        reg = sgbdSQL.consultar_db(script_sql)

        json_barema = resarcher_baremaSQL.researcher_production_db(
            "",
            Data["lattes_id"],
            year,
        )[0]

        json_barema["first_doc"] = str(reg[0][0])
        json_barema["id"] = Data["id"]
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
