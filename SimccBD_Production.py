import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import Dao.areaFlowSQL as areaFlowSQL
import Dao.termFlowSQL as termFlowSQL
import logging
import project
import sys


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

    filter = str()
    if teste == 1:
        filter = " WHERE r.id='c932854c-c5c6-4998-9f10-5b50cfb1366b'"

    reg = sgbdSQL.consultar_db("SELECT id from researcher r" + filter)

    df_bd = pd.DataFrame(reg, columns=["id"])

    for Index, infos in df_bd.iterrows():
        print(infos.id)
        print("total=%d" % Index)
        logger.debug("total=" + str(Index))
        new_researcher_production_db(infos.id)


def new_researcher_production_db(researcher_id):

    sql = f"""
        SELECT 
            count(title) as qtd, 
            b.type as tipo 
        FROM 
            bibliographic_production AS b 
        WHERE 
            researcher_id='{researcher_id}'
        GROUP BY 
            tipo 
        ORDER BY 
            tipo 
        DESC;
            """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["qtd", "tipo"])

    qtd_article = 0
    qtd_book_chapters = 0
    qtd_book = 0
    qtd_work_in_event = 0

    for i, infos in df_bd.iterrows():
        if infos.tipo == "BOOK":
            qtd_book = infos.qtd
        if infos.tipo == "WORK_IN_EVENT":
            qtd_work_in_event = infos.qtd
        if infos.tipo == "ARTICLE":
            qtd_article = infos.qtd
        if infos.tipo == "BOOK_CHAPTER":
            qtd_book_chapters = infos.qtd

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

    sql = f"""
        INSERT into public.researcher_production (
            researcher_id,
            articles,
            book_chapters,
            book,
            work_in_event,
            great_area,
            area_specialty,
            city,
            organ,
            patent,
            software,
            brand
        ) 
        values(
            '{researcher_id}',
            '{qtd_article}',
            '{qtd_book_chapters}',
            '{qtd_book}',
            '{qtd_work_in_event}',
            '{area}',
            '{speciality}',
            '{city}',
            '{organ}',
            '{qtd_patent}',
            '{qtd_software}',
            '{qtd_brand}'
        );
        """

    logger.debug(sql)
    sgbdSQL.execScript_db(sql)

    return df_bd


if __name__ == "__main__":
    try:
        project.project_env = sys.argv[1]
    except:
        project.project_env = str(input("Código do banco que sera utilizado [1-8]: "))
    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(
        filename="logfile_Production.log",
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger()

    create_researcher_production_db(0)
