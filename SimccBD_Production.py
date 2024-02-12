import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import psycopg2
import Dao.areaFlowSQL as areaFlowSQL
import Dao.util as util
import Dao.termFlowSQL as termFlowSQL
import logging
from datetime import datetime, timedelta

import project

import sys

project.project_env = sys.argv[1]


# Função processar e inserir a produção de cada pesquisador
def researcher_software_db(researcher_id):
    reg = sgbdSQL.consultar_db(
        "SELECT  count(*) as qtd from software AS s "
        + "  where researcher_id='"
        + researcher_id
        + "' "
    )
    df_bd = pd.DataFrame(reg, columns=["qtd"])
    qtd_software = 0
    if len(df_bd.index) >= 1:
        qtd_software = df_bd["qtd"].iloc[0]

    return qtd_software


# Função processar e inserir a produção de cada pesquisador
def researcher_brand_db(researcher_id):
    reg = sgbdSQL.consultar_db(
        "SELECT  count(*) as qtd from brand AS b "
        + "  where researcher_id='"
        + researcher_id
        + "' "
    )
    df_bd = pd.DataFrame(reg, columns=["qtd"])
    qtd_brand = 0
    if len(df_bd.index) >= 1:
        qtd_brand = df_bd["qtd"].iloc[0]

    return qtd_brand


# Função processar e inserir a produção de cada pesquisador
def researcher_patent_db(researcher_id):
    reg = sgbdSQL.consultar_db(
        "SELECT  count(*) as qtd from patent AS p "
        + "  where researcher_id='"
        + researcher_id
        + "' "
    )
    df_bd = pd.DataFrame(reg, columns=["qtd"])
    qtd_patent = 0
    if len(df_bd.index) >= 1:
        qtd_patent = df_bd["qtd"].iloc[0]

    return qtd_patent


# Função para listar todos os pesquisadores e criar a sua produção
def create_researcher_production_db(teste):

    sql = "DELETE FROM researcher_production"
    sgbdSQL.execScript_db(sql)

    filter = ""
    if teste == 1:
        filter = " WHERE r.id='c932854c-c5c6-4998-9f10-5b50cfb1366b'"
        # + " OR " +researcher_teste2

    reg = sgbdSQL.consultar_db("SELECT id from researcher r" + filter)

    df_bd = pd.DataFrame(reg, columns=["id"])
    x = 0

    for i, infos in df_bd.iterrows():
        print(infos.id)

        x = x + 1
        print("total=%d" % x)
        logger.debug("total=" + str(x))
        new_researcher_production_db(infos.id)


# Função processar e inserir a produção de cada pesquisador
def new_researcher_production_db(researcher_id):

    sql = (
        "SELECT  count(title) as qtd,b.type as tipo from bibliographic_production AS b  where researcher_id='%s' GROUP BY tipo ORDER  BY Tipo desc"
        % (researcher_id)
    )

    reg = sgbdSQL.consultar_db(sql)

    print(sql)
    df_bd = pd.DataFrame(reg, columns=["qtd", "tipo"])
    print(df_bd)
    # print(df_bd)
    qtd_article = 0
    qtd_book_chapters = 0
    qtd_book = 0
    qtd_work_in_event = 0

    for i, infos in df_bd.iterrows():

        print(infos.tipo)
        print(infos.qtd)

        if infos.tipo == "BOOK":
            qtd_book = infos.qtd

        if infos.tipo == "WORK_IN_EVENT":
            qtd_work_in_event = infos.qtd

        if infos.tipo == "ARTICLE":
            qtd_article = infos.qtd

        if infos.tipo == "BOOK_CHAPTER":
            qtd_book_chapters = infos.qtd

    """reg = consultar_db('SELECT  count(distinct title) as qtd from bibliographic_production AS b, bibliographic_production_author AS ba '+
                        ' WHERE b.id = ba.bibliographic_production_id '+
                        ' AND researcher_id=\''+researcher_id+"'"+
                        ' AND "type" = \'BOOK_CHAPTER\'')

     df_bd = pd.DataFrame(reg, columns=['qtd'])

     qtd_book_chapter = df_bd['qtd'].iloc[0]  

     """

    area = areaFlowSQL.lists_great_area_expertise_researcher_db(researcher_id)
    speciality = areaFlowSQL.lists_area_speciality_researcher_db(researcher_id)
    qtd_patent = researcher_patent_db(researcher_id)
    qtd_software = researcher_software_db(researcher_id)
    qtd_brand = researcher_brand_db(researcher_id)

    df_bd = termFlowSQL.get_researcher_address_db(researcher_id)

    city = ""
    organ = ""
    if len(df_bd.index) >= 1:
        city = df_bd["city"].iloc[0]
        organ = df_bd["organ"].iloc[0]

    sql = """
        INSERT into public.researcher_production (researcher_id ,articles,book_chapters,book,work_in_event,great_area,area_specialty,city,organ,patent,software,brand) 
        values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');
        """ % (
        researcher_id,
        qtd_article,
        qtd_book_chapters,
        qtd_book,
        qtd_work_in_event,
        area,
        speciality,
        city,
        organ,
        qtd_patent,
        qtd_software,
        qtd_brand,
    )
    # logger.debug(sql)
    print(sql)
    logger.debug(sql)
    sgbdSQL.execScript_db(sql)

    return df_bd


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename="logfile_Production.log",
    filemode="w",
    format=Log_Format,
    level=logging.DEBUG,
)

logger = logging.getLogger()


create_researcher_production_db(0)
