from datetime import datetime, timedelta
import Dao.resarcher_baremaSQL as resarcher_baremaSQL
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
from datetime import datetime
from Model.Year_Barema import Year_Barema
import sys

import project


def researcher_csv_db():

    sql = """
        SELECT 
            r.lattes_id
        FROM  
            researcher r"""

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["lattes_id"])

    df_bd.to_csv("researcher_simmc.csv")


def testeDiciopnario():

    sql = """

            SELECT translate(unaccent(LOWER(b.title)),':;''','') ::tsvector as palavras from researcher r,bibliographic_production b 
            WHERE b.researcher_id='9d22c219-d3f7-46a3-b4c3-ef04eb9c31c8' and b.type='ARTICLE'
        """
    reg = sgbdSQL.consultar_db(sql)

    # logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["palavras"])

    for i, infos in df_bd.iterrows():
        print(infos.palavras)


def dataLattes(dias):

    dataP = datetime.today() - timedelta(days=dias)

    sql = """

         SELECT  name from researcher where last_update <='%s'
      """ % (
        dataP
    )
    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["name_"])

    for i, infos in df_bd.iterrows():
        print(infos.name_)


if __name__ == "__main__":
    try:
        project.project_env = sys.argv[1]
    except:
        project.project_env = str(input("Código do banco que sera utilizado [1-8]: "))

    year = Year_Barema()
    year.article = "2014"
    year.work_event = "2014"
    year.book = "2014"
    year.chapter_book = "2014"
    year.patent = "2014"
    year.software = "2014"
    year.brand = "2014"
    year.resource_progress = "2014"
    year.resource_completed = "2014"
    year.participation_events = "2014"

    script_sql = """
    SELECT lattes_id FROM researcher;
    """

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame_lattes = pd.DataFrame(reg, columns=["lattes_id"])

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

        json_barema["first_doc"] = reg[0][0]

        lista.append(json_barema)

    data_frame_dados = pd.DataFrame(lista)

    data_frame_dados.to_csv("Files/researcher_group.csv")
