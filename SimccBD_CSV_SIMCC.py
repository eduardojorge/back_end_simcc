import Dao.sgbdSQL as sgbdSQL
import Dao.graduate_programSQL as graduate_programSQL
import pandas as pd
import logging
import json
from datetime import datetime
import sys

import project

dir = host_ = sys.argv[2]
try:
    project.project_env = sys.argv[1]
except:
    project.project_env = str(
        input("Código do banco que sera utilizado [1-8]: "))


def fat_simcc_bibliographic_production():

    sql = """

                                 SELECT distinct  title,b.type as tipo,b.researcher_id,year,i.id,
									                 bar.qualis,bar.periodical_magazine_name,bar.jcr,bar.jcr_link,
									                 c.id
                               from bibliographic_production AS b LEFT JOIN bibliographic_production_article bar ON b.id = bar.bibliographic_production_id,
										      researcher r  LEFT JOIN  institution i ON r.institution_id = i.id  LEFT JOIN  city c ON r.city_id = c.id

                                where b.researcher_id is not null 
                                  AND r.id =  b.researcher_id 
                                  ORDER  BY  YEAR desc
    """

    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "title",
            "tipo",
            "researcher_id",
            "year",
            "institution_id",
            "qualis",
            "periodical_magazine_name",
            "jcr",
            "jcr_link",
            "city_id",
        ],
    )

    df_bd.to_csv(dir + "fat_simcc_bibliographic_production.csv")


def dim_researcher_csv_db():

    sql = """ SELECT r.name AS researcher, r.id AS researcher_id, TO_CHAR(r.last_update,'dd/mm/yyyy') date_,r.graduation as graduation
        

        FROM  researcher r """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg, columns=["researcher", "researcher_id",
                      "last_update", "graduation"]
    )

    df_bd.to_csv(dir + "dim_researcher.csv")


def dim_institution_csv_db():

    sql = """   SELECT i.id,i.name, i.acronym  FROM institution i """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["institution_id", "name", "acronym "])

    df_bd.to_csv(dir + "dim_institution.csv")


def dim_city_csv_db():

    sql = """   SELECT c.id,c.name  FROM city c """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["city_id", "name"])

    df_bd.to_csv(dir + "dim_city.csv")


# Função processar e inserir a produção de cada pesquisador
def fat_production_tecnical_year_novo_csv_db():

    sql = """
              SELECT distinct title,development_year::int AS year, 'PATENTE' as TYPE,p.researcher_id,r.city_id,r.institution_id
          FROM patent p, researcher r
          WHERE 
          r.id = p.researcher_id
         

          UNION
          SELECT distinct title,s.year as year,'SOFTWARE',researcher_id,r.city_id,r.institution_id
          from software s, researcher r
          WHERE 
          r.id = s.researcher_id
         
          UNION
          SELECT distinct title,b.year as year,'MARCA',researcher_id,r.city_id,r.institution_id
          from brand b,researcher r
          WHERE 
          r.id = b.researcher_id
          UNION
          SELECT distinct title,b.year as year,'RELATÓRIO TÉCNICO',researcher_id,r.city_id,r.institution_id
          from research_report b,researcher r
           WHERE 
           r.id = b.researcher_id


               

    """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=["title", "year", "type",
                 "researcher_id", "city_id", "institution_id"],
    )

    df_bd.to_csv(dir + "fat_production_tecnical_year_novo_csv_db.csv")


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename="logfile_csv.log", filemode="w", format=Log_Format, level=logging.DEBUG
)

logger = logging.getLogger()


logger.debug("Inicio")
list_data = []
hoje = str(datetime.now())
print(hoje)

data = {"data": hoje}

list_data.append(data)
json_string = json.dumps(list_data)
df = pd.read_json(json_string)
df.to_csv(dir + "data.csv")


print("Inicio: fat_simcc_bibliographic_production")
fat_simcc_bibliographic_production()
print("Fim:fat_simcc_bibliographic_production")

print("Inicio:dim_researcher_csv_db")
dim_researcher_csv_db()
print("Fim: dim_researcher_csv_db")

print("Inicio:dim_institution_csv_db")
dim_institution_csv_db()
print("Fim: dim_institution_csv_db")

print("Inicio:dim_city_csv_db")
dim_city_csv_db()
print("Fim: dim_city_csv_db")

print("Inicio:fat_production_tecnical_year_novo_csv_db()")
fat_production_tecnical_year_novo_csv_db()
print("Fim: fat_production_tecnical_year_novo_csv_db()")
