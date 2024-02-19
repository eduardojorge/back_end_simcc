from nltk.tokenize import RegexpTokenizer
from datetime import datetime, timedelta
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import logging
import project
import nltk
import sys


def create_researcher_dictionary_db(test, article, abstract, patent, event):

    filter = str()
    if test == 1:
        filter = " OFFSET 0 ROWS FETCH FIRST 3 ROW ONLY"

    sql = "SELECT distinct  r.id from researcher r  " + filter
    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["id"])

    if article == 1:
        sql = "DELETE FROM research_dictionary where type_='ARTICLE'"

        sgbdSQL.execScript_db(sql)
        for i, infos in df_bd.iterrows():

            if (i % 100) == 0:
                print("Total Pesquisador Article: " + str(i))
                logger.debug("Total Pesquisador Article: " + str(i))

            create_researcher_title_dictionary_db(infos.id)

    # Abstract
    if abstract == 1:
        sql = "DELETE FROM research_dictionary where type_='ABSTRACT'"
        sgbdSQL.execScript_db(sql)
        for i, infos in df_bd.iterrows():
            if (i % 100) == 0:
                print("Total Pesquisador Abstract: " + str(i))
                logger.debug("Total Pesquisador abstract: " + str(i))

            create_researcher_abstract_dictionary_db(infos.id)

    if patent == 1:
        sql = "DELETE FROM research_dictionary where type_='PATENT'"
        sgbdSQL.execScript_db(sql)
        for i, infos in df_bd.iterrows():
            if (i % 100) == 0:
                print("Total Pesquisador Patent: " + str(i))
                logger.debug("Total Pesquisador Patent: " + str(i))

            create_researcher_patent_dictionary_db(infos.id)

    if event == 1:
        sql = "DELETE FROM research_dictionary where type_='SPEAKER'"
        sgbdSQL.execScript_db(sql)
        for i, infos in df_bd.iterrows():
            if (i % 100) == 0:
                print("Total Pesquisador Participacao Evento: " + str(i))
                logger.debug("Total Pesquisador Participacao Evento: " + str(i))

            create_researcher_participation_events_dictionary_db(infos.id)


def create_researcher_title_dictionary_db(researcher_id):

    filter = " AND  type in ('ARTICLE','BOOK','BOOK_CHAPTER') "
    sql = (
        """ SELECT  distinct title,b.type,year from bibliographic_production AS b  WHERE researcher_id='%s' %s """
        % (researcher_id, filter)
    )

    reg = sgbdSQL.consultar_db(sql)

    logger.debug("Entrei nos artigos pesquisador " + researcher_id)
    df_bd = pd.DataFrame(reg, columns=["title", "type_", "year"])

    for i, infos in df_bd.iterrows():

        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)
        insert_research_dictionary_db(tokens, infos.type_)


def create_researcher_abstract_dictionary_db(researcher_id):

    reg = sgbdSQL.consultar_db(
        "SELECT  r.abstract as abstract from researcher as r"
        + " WHERE "
        + "r.id='"
        + researcher_id
        + "'"
    )

    df_bd = pd.DataFrame(reg, columns=["abstract"])
    for i, infos in df_bd.iterrows():
        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        if infos.abstract is not None:
            tokens = tokenize.tokenize(infos.abstract)
            insert_research_dictionary_db(tokens, "ABSTRACT")


def create_researcher_patent_dictionary_db(researcher_id):

    sql = (
        """SELECT distinct title,development_year  as  year  from patent AS b  WHERE researcher_id='%s' """
        % researcher_id
    )

    reg = sgbdSQL.consultar_db(sql)

    logger.debug("Entrei nas patentes  " + researcher_id)
    df_bd = pd.DataFrame(reg, columns=["title", "year"])

    for i, infos in df_bd.iterrows():

        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)

        insert_research_dictionary_db(tokens, "PATENT")


def create_researcher_participation_events_dictionary_db(researcher_id):

    sql = """SELECT  distinct event_name,year  as  year  from participation_events AS p
          WHERE  type_participation in ('Apresentação Oral','Conferencista','Moderador','Simposista')   and  p.researcher_id='%s' """ % (
        researcher_id
    )

    reg = sgbdSQL.consultar_db(sql)

    logger.debug("Entrei nos eventos  " + researcher_id)
    df_bd = pd.DataFrame(reg, columns=["title", "year"])

    for i, infos in df_bd.iterrows():

        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)

        insert_research_dictionary_db(tokens, "SPEAKER")


def insert_research_dictionary_db(tokens, type):

    stopwords_portuguese = nltk.corpus.stopwords.words("portuguese")
    stopwords_english = nltk.corpus.stopwords.words("english")

    for word in tokens:
        if len(word) >= 3:
            if not (
                (word.lower() in stopwords_portuguese)
                or (word.lower() in stopwords_english)
            ):

                try:

                    sql = """
                      INSERT into public.research_dictionary  (term,frequency,type_)  VALUES ('%s',1,'%s') 
                      ON CONFLICT (term,type_) 
                      DO 
                      UPDATE  SET  frequency=(select frequency from research_dictionary WHERE term= EXCLUDED.term AND type_=EXCLUDED.type_) +1 ;
     			            """ % (
                        word.lower(),
                        type,
                    )

                    sgbdSQL.execScript_db(sql)

                # else:
                except Exception as e:

                    print(e)
                    logger.debug(e)


if __name__ == "__main__":

    try:
        project.project_env = sys.argv[1]
    except:
        project.project_env = str(input("Código do banco que sera utilizado [1-8]: "))

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(
        filename="logfile_Population.log",
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )
    logger = logging.getLogger()

    logger.debug("Inicio")

    script_sql = """
        UPDATE 
            bibliographic_production_article ba 
        SET 
            qualis = 'A4' 
        WHERE 
            ba.issn = '17412242'
        """

    sgbdSQL.execScript_db(script_sql)

    logger.debug(script_sql)

    script_sql = """
        UPDATE  
            bibliographic_production_article p 
        SET 
            jcr=(subquery.jif2019),
            jcr_link=url_revista
        FROM 
            (
            SELECT
                jif2019,
                eissn,
                url_revista
            FROM 
                "JCR_novo_link_v1"
            ) AS subquery
        WHERE 
            translate(subquery.eissn,'-','') = p.issn
        """

    sgbdSQL.execScript_db(script_sql)

    logger.debug(script_sql)

    script_sql = """
    UPDATE  
        bibliographic_production_article p 
    SET 
        jcr = (subquery.jif2019),
        jcr_link=url_revista
    FROM 
        (
        SELECT 
            jif2019,
            issn,
            url_revista
        FROM  
            "JCR_novo_link_v1"
        ) AS subquery
    WHERE
        translate(subquery.issn,'-','')=p.issn
    """

    sgbdSQL.execScript_db(script_sql)
    logger.debug(script_sql)

    script_sql = """
        UPDATE 
            bibliographic_production 
        SET 
        YEAR_=YEAR::INTEGER
        """

    sgbdSQL.execScript_db(script_sql)
    logger.debug(script_sql)

    script_sql = """
        UPDATE 
            bibliographic_production_article  
        SET 
            qualis='B2' 
        WHERE 
            issn='26748568' 
            OR issn='2764622'
        """

    sgbdSQL.execScript_db(script_sql)
    logger.debug(script_sql)

    script_sql = """
        UPDATE bibliographic_production
        SET title = translate(title, '''', ' ')
        """
    sgbdSQL.execScript_db(script_sql)
    logger.debug(script_sql)

    TESTE = 0

    create_researcher_dictionary_db(TESTE, 1, 1, 1, 1)
