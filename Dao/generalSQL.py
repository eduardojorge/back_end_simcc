import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util


# Função que lista  as áreas de expertrize por Iniciais


def listSecondWord_bd(term):
    sql = """

           SELECT   
                  unnest(regexp_matches (unaccent(LOWER(bp.title)), ' %s\s+(\w+)','g')) as word FROM bibliographic_production AS bp where bp.type='ARTICLE'
         """ % (
        term
    )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["word"])

    return df_bd


def lists_magazine_db(initials, issn):
    filter = ""
    if initials != "":
        filter = " LOWER(name) like '" + initials.lower() + "%' "
    filteIssn = ""
    if issn != "":
        filteIssn = " translate(issn,'-','')='" + issn + "'"

    if (filter == "") and (filteIssn == ""):
        sql = """
            SELECT m.id as id, m.name as magazine, issn,jcr,jcr_link,qualis  
            FROM  periodical_magazine  m ORDER BY name asc
            """
    else:
        sql = """SELECT m.id as id, m.name as magazine, issn,jcr,jcr_link,qualis
                            
            FROM  periodical_magazine  m
                           where 
                            %s %s
                           
                           ORDER BY name asc""" % (
            filter,
            filteIssn,
        )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "magazine", "issn", "jcr", "jcr_link", "qualis"]
    )

    return df_bd


def queryCity():
    return pd.DataFrame(
        sgbdSQL.consultar_db("SELECT id, name, country_id, state_id FROM city;"),
        columns=["id", "name", "country_id", "state_id"],
    )
