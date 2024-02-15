from nltk.tokenize import RegexpTokenizer
from datetime import datetime, timedelta
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import traceback
import logging
import project
import nltk
import time

dataP = datetime.today() - timedelta(days=100000)

import sys

project.project_env = sys.argv[1]
researcher_teste1 = "r.name LIKE 'Manoel %' OR r.name LIKE 'Gesil Sampaio%' "


def insert_researcher_frequency_db(teste, article, offset):
    time.sleep(3)
    filter = ""
    if teste == True:
        filter = f" and  b.created_at>= '{dataP}'"

    reg = sgbdSQL.consultar_db(
        "SELECT  distinct  r.id from researcher r, bibliographic_production b where r.id =b.researcher_id "
        + filter
        + " OFFSET "
        + str(offset)
        + " ROWS FETCH FIRST 100 ROW ONLY"
    )
    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
    df_bd = pd.DataFrame(reg, columns=["id"])

    for i, infos in df_bd.iterrows():
        researcher_id = infos.id
        print(infos.id)
        print((i + offset))
        logger.debug(SCRIPT_SQL)
        insert_researcher_frequency_caracter_bd(researcher_id, article)
        insert_researcher_abstract_frequency_caracter_bd(researcher_id)
        insert_researcher_patent_frequency_caracter_bd(researcher_id)
        logger.debug("Inserindo insert_researcher_frequency_db  " + str(researcher_id))

        # a = list(string.ascii_lowercase)


def insert_researcher_frequency_caracter_bd(researcher_id, article):

    try:

        sql = """
           
	         INSERT into public.researcher_frequency (term,researcher_id,bibliographic_production_id) 
	  
	  
	           SELECT   unaccent(r.term) as term,b.researcher_id as researcher_id ,b.id as bibliographic_production_id 
                                 FROM research_dictionary as r,bibliographic_production AS b   
                                 WHERE
                                  
                                (        translate(unaccent(LOWER(b.title)),':;''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              AND type='ARTICLE'
                                
                                 AND b.researcher_id='%s' AND r.type_='ARTICLE' 
        """ % (
            researcher_id
        )

        sgbdSQL.execScript_db(sql)
        # logger.debug(sql)

    except Exception as e:
        print(e)
        logger.error(e)
        traceback.print_exc()


def insert_researcher_abstract_frequency_caracter_bd(researcher_id):

    # print(caracter)
    try:

        sql = """
             INSERT into public.researcher_abstract_frequency (researcher_id,term) 
                 
                  SELECT  re.id, unaccent(r.term)
                                FROM research_dictionary as r,researcher as re
                                 WHERE 
                                (        translate(unaccent(LOWER(re.abstract)),':;\''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              
                             
                                  AND re.id= '%s' 
                                  AND r.type_='ABSTRACT' 
         
         """ % (
            researcher_id
        )

        sgbdSQL.execScript_db(sql)

    except Exception as ERROR:
        print(ERROR)
        traceback.print_exc()


def insert_researcher_patent_frequency_caracter_bd(researcher_id):

    # print(caracter)
    try:

        sql = """
             INSERT into public.researcher_patent_frequency (term,researcher_id,patent_id) 
	  
	  
	           SELECT   unaccent(r.term) as term,b.researcher_id as researcher_id ,b.id as patent_id 
                                 FROM research_dictionary as r,patent AS b   
                                 WHERE
                                  
                                (        translate(unaccent(LOWER(b.title)),':;''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              
                             
                                  AND b.researcher_id= '%s' 
                                  AND r.type_='PATENT' 
         
         """ % (
            researcher_id
        )

        sgbdSQL.execScript_db(sql)

    except Exception as e:
        print(e)
        traceback.print_exc()


# Função para consultar o Banco SIMCC
def create_researcher_dictionary_db(test, article, abstract, patent, event):

    filter = ""
    if test == 1:
        filter = " OFFSET 0 ROWS FETCH FIRST 3 ROW ONLY"

    sql = "SELECT distinct  r.id from researcher r  " + filter
    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["id"])

    # Article
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

    # Patent

    if patent == 1:
        sql = "DELETE FROM research_dictionary where type_='PATENT'"
        sgbdSQL.execScript_db(sql)
        for i, infos in df_bd.iterrows():
            if (i % 100) == 0:
                print("Total Pesquisador Patent: " + str(i))
                logger.debug("Total Pesquisador Patent: " + str(i))

            create_researcher_patent_dictionary_db(infos.id)

    # Event
    # Abstract
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
    # print(df_bd.head())
    texto = ""

    for i, infos in df_bd.iterrows():
        # print(infos.title)
        # print(infos.year)
        # Retirando a pontuação
        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)
        # print(infos.title)

        insert_research_dictionary_db(tokens, infos.type_)


def create_researcher_participation_events_dictionary_db(researcher_id):
    filter = ""

    sql = """SELECT  distinct event_name,year  as  year  from participation_events AS p
          WHERE  type_participation in ('Apresentação Oral','Conferencista','Moderador','Simposista')   and  p.researcher_id='%s' """ % (
        researcher_id
    )

    reg = sgbdSQL.consultar_db(sql)

    logger.debug("Entrei nos eventos  " + researcher_id)
    df_bd = pd.DataFrame(reg, columns=["title", "year"])

    for i, infos in df_bd.iterrows():
        # print(infos.title)
        # print(infos.year)
        # Retirando a pontuação
        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)
        # print(infos.title)

        insert_research_dictionary_db(tokens, "SPEAKER")


def create_researcher_patent_dictionary_db(researcher_id):

    sql = (
        """SELECT  distinct title,development_year  as  year  from patent AS b  WHERE researcher_id='%s' """
        % researcher_id
    )

    reg = sgbdSQL.consultar_db(sql)

    logger.debug("Entrei nas patentes  " + researcher_id)
    df_bd = pd.DataFrame(reg, columns=["title", "year"])
    # print(df_bd.head())
    texto = ""

    for i, infos in df_bd.iterrows():
        # print(infos.title)
        # print(infos.year)
        # Retirando a pontuação
        tokenize = RegexpTokenizer(r"\w+")
        tokens = []
        tokens = tokenize.tokenize(infos.title)
        # print(infos.title)

        insert_research_dictionary_db(tokens, "PATENT")


def create_researcher_abstract_dictionary_db(researcher_id):

    reg = sgbdSQL.consultar_db(
        "SELECT  r.abstract as abstract from researcher as r"
        + " WHERE "
        +
        # update_abstract=true and
        "r.id='"
        + researcher_id
        + "'"
    )

    df_bd = pd.DataFrame(reg, columns=["abstract"])
    # print(df_bd.head())
    texto = ""
    x = 0
    for i, infos in df_bd.iterrows():
        # print(infos.title)
        # print(infos.year)
        # Retirando a pontuação
        tokenize = RegexpTokenizer(r"\w+")
        tokens = []

        # print(infos.abstract)
        if infos.abstract is not None:
            tokens = tokenize.tokenize(infos.abstract)
            # print(infos.title)

            insert_research_dictionary_db(tokens, "ABSTRACT")

    i1 = 0


def insert_research_dictionary_db(tokens, type):

    stopwords_portuguese = nltk.corpus.stopwords.words("portuguese")
    stopwords_english = nltk.corpus.stopwords.words("english")

    word_previous = ""
    for word in tokens:
        if len(word) >= 3:
            if not (
                (word.lower() in stopwords_portuguese)
                or (word.lower() in stopwords_english)
            ):

                # sql="SELECT count(*) as total FROM research_dictionary WHERE type_='%s' AND term='%s'" % (type,word.lower())
                # reg = sgbdSQL.consultar_db(sql)
                # df_bd = pd.DataFrame(reg, columns=['total'])

                # for i,infos in df_bd.iterrows():
                # if (infos.total==0):
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


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename="logfile_Population.log",
    filemode="w",
    format=Log_Format,
    level=logging.DEBUG,
)

logger = logging.getLogger()


logger.debug("Inicio")
try:

    # lattes10.lattes_10_researcher_frequency_db(logger)
    x = 1
except Exception as e:
    print(e)
    traceback.print_exc()

SCRIPT_SQL = """

UPDATE bibliographic_production_article ba SET qualis='A4' WHERE ba.issn='17412242'

"""
sgbdSQL.execScript_db(SCRIPT_SQL)
logger.debug(SCRIPT_SQL)

SCRIPT_SQL = """

UPDATE  bibliographic_production_article p SET jcr=(subquery.jif2019),jcr_link=url_revista
FROM (SELECT jif2019,eissn,url_revista
      FROM  "JCR_novo_link_v1" ) AS subquery
WHERE translate(subquery.eissn,'-','')=p.issn
"""
sgbdSQL.execScript_db(SCRIPT_SQL)

logger.debug(SCRIPT_SQL)

SCRIPT_SQL = """
UPDATE  bibliographic_production_article p SET jcr=(subquery.jif2019),jcr_link=url_revista
FROM (SELECT jif2019,issn,url_revista
      FROM  "JCR_novo_link_v1" ) AS subquery
WHERE translate(subquery.issn,'-','')=p.issn
"""
sgbdSQL.execScript_db(SCRIPT_SQL)

logger.debug(SCRIPT_SQL)


# print(areaFlowSQL.lists_area_speciality_researcher_db('215a5c60-d882-4936-9445-da4742c14802'))
SCRIPT_SQL = """
      UPDATE bibliographic_production SET YEAR_=YEAR::INTEGER
        """

sgbdSQL.execScript_db(SCRIPT_SQL)
logger.debug(SCRIPT_SQL)


SCRIPT_SQL = """ UPDATE bibliographic_production_article  SET qualis='B2' WHERE issn='26748568' OR issn='2764622'"""

sgbdSQL.execScript_db(SCRIPT_SQL)

logger.debug(SCRIPT_SQL)


print("Passo II")

TESTE = 0

create_researcher_dictionary_db(TESTE, 0, 0, 0, 1)
