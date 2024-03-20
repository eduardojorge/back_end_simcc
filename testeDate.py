from datetime import datetime, timedelta
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
from datetime import datetime


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
