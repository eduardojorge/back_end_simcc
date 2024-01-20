from datetime import datetime, timedelta
import Dao.resarcher_baremaSQL as resarcher_baremaSQL
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
from datetime import datetime

import project as project_
import sys
from Model.Year_Barema import Year_Barema

project_.project_ = "7"


def researcher_csv_db():
    sql = """ SELECT r.lattes_id
        

        FROM  researcher r """

    reg = sgbdSQL.consultar_db(sql)

    # logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["lattes_id"])

    df_bd.to_csv("researcher_simmc.csv")


def testeDiciopnario():
    sql = """

            SELECT   translate(unaccent(LOWER(b.title)),':;''','') ::tsvector as palavras from researcher r,bibliographic_production b 
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

    # logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["name_"])

    for i, infos in df_bd.iterrows():
        print(infos.name_)


def bigrama():
    reg = sgbdSQL.consultar_db(
        "SELECT  distinct  b.title from researcher r, bibliographic_production b where r.id =b.researcher_id "
        + filter
        + " OFFSET "
        + str(offset)
        + " ROWS FETCH FIRST 100 ROW ONLY"
    )
    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
    df_bd = pd.DataFrame(reg, columns=["id"])


# hoje = datetime.today() - timedelta(days=5)
# print(hoje.date())

# dataLattes(180)

# testeDiciopnario()
# researcher_csv_db()

# "1966167015825708;8933624812566216"


year = Year_Barema()
year.article = "2018"
year.work_event = "2018"
year.book = "1900"

year.chapter_book = "1900"

year.patent = "1900"
year.software = "1900"
year.brand = "1900"
year.resource_progress = "1900"
year.resource_completed = "1900"
year.participation_events = "1900"


print(resarcher_baremaSQL.researcher_production_db("todos", "", year))
