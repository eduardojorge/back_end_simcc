import Dao.sgbdSQL as sgbdSQL
import pandas as pd
from datetime import datetime
import sys
import logging
import json
from dotenv import load_dotenv

load_dotenv(override=True)


def fat_simcc_bibliographic_production():

    sql = """
        SELECT 
            distinct title,
            b.type as tipo,
            b.researcher_id,
            year,
            i.id,
            bar.qualis,
            bar.periodical_magazine_name,
            bar.jcr,
            bar.jcr_link,
            c.id
        FROM 
            bibliographic_production AS b 
        LEFT JOIN bibliographic_production_article bar
            ON b.id = bar.bibliographic_production_id, researcher r  
        LEFT JOIN  institution i 
            ON r.institution_id = i.id 
        LEFT JOIN city c 
            ON r.city_id = c.id
        WHERE 
            b.researcher_id IS NOT NULL 
            AND r.id =  b.researcher_id 
        ORDER BY 
            YEAR desc;
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

    sql = """
        SELECT 
            r.name AS researcher, 
            r.id AS researcher_id, 
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS date_,
            r.graduation AS graduation,
            r.institution_id
        FROM  
            researcher r
        """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "researcher",
            "researcher_id",
            "last_update",
            "graduation",
            "institution_id",
        ],
    )

    df_bd.to_csv(dir + "dim_researcher.csv")


def dim_institution_csv_db():

    sql = """
        SELECT 
            i.id,
            i.name,
            i.acronym  
        FROM 
            institution i
        """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["institution_id", "name", "acronym "])

    df_bd.to_csv(dir + "dim_institution.csv")


def dim_city_csv_db():

    sql = """
        SELECT 
            c.id,
            c.name  
        FROM 
            city c"""

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["city_id", "name"])

    df_bd.to_csv(dir + "dim_city.csv")


def fat_production_tecnical_year_novo_csv_db():

    sql = """
        SELECT 
            DISTINCT title,
            development_year::int AS year,
            'PATENTE' AS TYPE,
            p.researcher_id,
            r.city_id,
            r.institution_id
        FROM 
            patent p, researcher r
        WHERE 
            r.id = p.researcher_id
        UNION
        SELECT 
            DISTINCT title,
            s.year AS year,
            'SOFTWARE' AS TYPE,
            researcher_id,
            r.city_id,
            r.institution_id
        FROM 
            software s, researcher r
        WHERE 
            r.id = s.researcher_id
        UNION
        SELECT 
            DISTINCT title,
            b.year AS year,
            'MARCA' AS TYPE,
            researcher_id,
            r.city_id,
            r.institution_id
        FROM 
            brand b, researcher r
        WHERE 
            r.id = b.researcher_id
        UNION
        SELECT 
            DISTINCT title,
            b.year AS year,
            'RELATÓRIO TÉCNICO' AS TYPE,
            researcher_id,
            r.city_id,
            r.institution_id
        FROM 
            research_report b, researcher r
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


def fat_foment():
    script_sql = """
        SELECT 
            r.id,
            r.institution_id,
            modality_code, 
            modality_name, 
            category_level_code, 
            funding_program_name,
            aid_quantity,
            scholarship_quantity
        FROM 
            public.subsidy s
            LEFT JOIN researcher r ON r.id = s.researcher_id;
        """

    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=[
        'researcher_id',
        'institution_id',
        'modality_code',
        'modality_name',
        'category_level_code',
        'funding_program_name',
        'aid_quantity',
        'scholarship_quantity',
    ])

    df.to_csv(dir + 'fat_foment.csv')


def dim_category_level_code():
    script_sql = """
    SELECT DISTINCT category_level_code FROM subsidy
    """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=['category_level_code'])

    df.to_csv(dir + 'dim_category_level_code.csv')


def dim_research_group():
    script_sql = '''
        SELECT
            rg.id,
            TRANSLATE(rg.name, '"', ''), 
            rg.area,
            i.id as institution_id
        FROM
            public.research_group_dgp rg
            LEFT JOIN institution i ON i.acronym = rg.institution
            WHERE rg.institution = 'UFMG'
        '''
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=[
        'group_id',
        'group_name',
        'area',
        'institution_id'])

    df.to_csv(dir + 'dim_research_group.csv')


def fat_group_leaders():
    script_sql = """
        SELECT 
            id,
            first_leader_id, 
            second_leader_id
        FROM
            public.research_group_dgp
        WHERE 
            first_leader_id IS NOT NULL
            OR second_leader_id IS NOT NULL
            """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=[
        'group_id',
        'first_leader_id',
        'second_leader_id'])

    df.to_csv(dir + 'fat_group_leaders.csv')


def fat_departament_csv_bd():
    script_sql = """
        SELECT 
            dep_id, 
            dep_nom, 
            'Escola de Engenharia', 
            '083a16f0-cccf-47d2-a676-d10b8931f66b'
        FROM 
            public.ufmg_departament
        """
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(
        reg, columns=['dep_id', 'dep_nom', 'institution', 'institution_id'])
    df_bd.to_csv(dir + 'dim_departament.csv')


def dim_departament_researcher():
    script_sql = """
        SELECT dep_id, researcher_id FROM public.departament_researcher
        """
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=['dep_id', 'researcher_id'])
    df_bd.to_csv(dir + 'dim_departament_researcher.csv')


def dim_departament_technician():
    script_sql = """
        SELECT dep_id, technician_id FROM
        ufmg_departament_technician
        """
    registry = sgbdSQL.consultar_db(script_sql)
    data_frame = pd.DataFrame(registry, columns=['dep_id', 'technician_id'])

    data_frame.to_csv(dir + 'dim_departament_technician.csv')


if __name__ == "__main__":

    dir = "Files/indicadores_simcc/"

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(
        filename="logfile_csv.log",
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
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

    print('Inicio: fat_foment()')
    fat_foment()
    print('Fim: fat_foment()')

    print('Inicio: dim_category_level_code()')
    dim_category_level_code()
    print('Fim: dim_category_level_code()')

    print('Inicio: dim_research_group()')
    dim_research_group()
    print('Fim: dim_research_group()')

    print('Inicio: fat_group_leaders()')
    fat_group_leaders()
    print('Fim: fat_group_leaders()')

    print('Inicio: fat_departament_csv_bd()')
    fat_departament_csv_bd()
    print('Fim: fat_departament_csv_bd()')

    print('Inicio: dim_departament_researcher()')
    dim_departament_researcher()
    print('Fim: dim_departament_researcher()')

    print('Inicio: dim_departament_technician()')
    dim_departament_technician()
    print('Fim: dim_departament_technician()')
