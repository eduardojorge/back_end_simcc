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
        project.project_env = str(
            input("Código do banco que sera utilizado [1-8]: "))

    qualis_barema = {'article_A1': 1.000, 'article_A2': 0.875, 'article_A3': 0.750, 'article_A4': 0.625,
                     'article_B1': 0.500, 'article_B2': 0.370, 'article_B3': 0.250, 'article_B4': 0.125}

    if int(input('Modificar os pesos para o indice de produção? [1 - Sim/0 - Não]')):
        for key, value in qualis_barema.items():
            qualis_barema[key] = int(
                input(f'Alterando o peso para produções({key}).\nDe {value} para: '))

    year = Year_Barema()

    year_input = str(input("Procurar produções(Artigos) a partir do ano: "))

    year.article = year_input

    year_input = str(
        input("Procurar demais produções(livros) a partir do ano: "))

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
    SELECT id, lattes_id FROM researcher;
    """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_lattes = pd.DataFrame(registry, columns=["id", "lattes_id"])

    lista = list()
    for Index, Data in data_frame_lattes.iterrows():

        json_barema = resarcher_baremaSQL.researcher_production_db(
            "",
            Data["lattes_id"],
            year,
        )[0]
        json_barema["id"] = Data["id"]

        # script_sql = f"""
        # SELECT
        #     MIN(e.education_end) as menor_education_end
        # FROM
        #     education e
        # JOIN researcher r
        # ON r.id = e.researcher_id
        # WHERE
        #     r.lattes_id = '{Data["lattes_id"]}'
        #     AND e.degree = 'DOUTORADO';
        # """
        # registry = sgbdSQL.consultar_db(script_sql)
        # json_barema["first_doc"] = str(registry[0][0])

        # script_sql = f"""
        #     SELECT
        #         r.institution_id
        #     FROM
        #         researcher r
        #     JOIN institution i
        #     ON i.id = r.institution_id
        #     WHERE
        #     r.lattes_id = '{Data["lattes_id"]}';
        #     """
        # registry = sgbdSQL.consultar_db(script_sql)
        # data_frame_institution = pd.DataFrame(
        #     registry, columns=['institution_id'])

        # json_barema['institution_id'] = data_frame_institution['institution_id'][0]

        ind_prod = 0

        for key, value in qualis_barema.items():
            if json_barema[key]:
                ind_prod += qualis_barema[key] * json_barema[key]

        json_barema['ind_prod'] = ind_prod
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
