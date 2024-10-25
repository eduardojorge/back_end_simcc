import Dao.sgbdSQL as sgbdSQL
import Dao.graduate_programSQL as graduate_programSQL
import pandas as pd
import logging
from dotenv import load_dotenv
import os
from datetime import datetime
import json
from io import StringIO


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

    df_bd.to_csv(csv_dir + "fat_simcc_bibliographic_production.csv")


def dim_researcher_csv_db():
    sql = """
        SELECT 
            r.name AS researcher, 
            r.id AS researcher_id, 
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS date_,
            r.graduation AS graduation,
            r.institution_id,
            r.docente
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
            "docente",
        ],
    )

    df_bd.to_csv(csv_dir + "dim_researcher.csv")


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

    df_bd.to_csv(csv_dir + "dim_institution.csv")


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

    df_bd.to_csv(csv_dir + "dim_city.csv")


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
        columns=["title", "year", "type", "researcher_id", "city_id", "institution_id"],
    )

    df_bd.to_csv(csv_dir + "fat_production_tecnical_year_novo_csv_db.csv")


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
            public.foment s
            LEFT JOIN researcher r ON r.id = s.researcher_id
        WHERE
            s.researcher_id IS NOT NULL
        """

    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        reg,
        columns=[
            "researcher_id",
            "institution_id",
            "modality_code",
            "modality_name",
            "category_level_code",
            "funding_program_name",
            "aid_quantity",
            "scholarship_quantity",
        ],
    )

    df.to_csv(csv_dir + "fat_foment.csv")


def dim_category_level_code():
    script_sql = """
    SELECT DISTINCT category_level_code FROM foment
    """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=["category_level_code"])

    df.to_csv(csv_dir + "dim_category_level_code.csv")


def dim_research_group():
    script_sql = """
        SELECT
            rg.id,
            TRANSLATE(rg.name, '"', '') AS name,
            rg.area,
            i.id AS institution_id
        FROM
            public.research_group rg
        RIGHT JOIN
            institution i ON rg.institution ILIKE '%' || i.acronym || '%'
        """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=["group_id", "group_name", "area", "institution_id"])

    df.to_csv(csv_dir + "dim_research_group.csv")


def fat_group_leaders():
    script_sql = """
        SELECT
            id,
            name,
            institution,
            first_leader,
            first_leader_id,
            second_leader,
            second_leader_id,
            AREA,
            census,
            start_of_collection,
            end_of_collection,
            group_identifier,
            YEAR,
            institution_name,
            category
        FROM
            public.research_group
        WHERE
            first_leader_id IS NOT NULL
            OR second_leader_id IS NOT NULL
            """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        reg,
        columns=[
            "id",
            "name",
            "institution",
            "first_leader",
            "first_leader_id",
            "second_leader",
            "second_leader_id",
            "AREA",
            "census",
            "start_of_collection",
            "end_of_collection",
            "group_identifier",
            "YEAR",
            "institution_name",
            "category",
        ],
    )

    df.to_csv(csv_dir + "fat_group_leaders.csv")


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
        reg, columns=["dep_id", "dep_nom", "institution", "institution_id"]
    )
    df_bd.to_csv(csv_dir + "dim_departament.csv")


def dim_departament_researcher():
    script_sql = """
        SELECT dep_id, researcher_id FROM public.departament_researcher
        """
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["dep_id", "researcher_id"])
    df_bd.to_csv(csv_dir + "dim_departament_researcher.csv")


def dim_departament_technician():
    script_sql = """
        SELECT dep_id, technician_id FROM
        ufmg_departament_technician
        """
    registry = sgbdSQL.consultar_db(script_sql)
    data_frame = pd.DataFrame(registry, columns=["dep_id", "technician_id"])

    data_frame.to_csv(csv_dir + "dim_departament_technician.csv")


def researcher_production_tecnical_year_csv_db():
    sql = """
          SELECT researcher_id,title,development_year::int AS YEAR, 'PATENT' as type FROM patent
          UNION
          SELECT researcher_id,title,YEAR,'SOFTWARE' from software
          UNION
          SELECT researcher_id,title,YEAR,'BRAND' from brand
          UNION
          SELECT researcher_id,title,YEAR,'REPORT' from research_report
    """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["researcher_id", "title", "year", "type"])

    logger.debug(sql)

    df_bd.to_csv(csv_dir + "production_tecnical_year.csv")


def researcher_production_year_csv_db():
    reg = sgbdSQL.consultar_db(
        "SELECT distinct  title,b.type as tipo,b.researcher_id,year,'institution' "
        + " from bibliographic_production AS b, researcher r  "
        + " where b.researcher_id is not null "
        + "   AND r.id =  b.researcher_id "
        + " GROUP BY title,tipo,b.researcher_id,year ORDER  BY year,Tipo desc"
    )

    df_bd = pd.DataFrame(
        reg, columns=["title", "tipo", "researcher_id", "year", "institution"]
    )

    df_bd.to_csv(csv_dir + "production_year.csv")


def researcher_production_year_distinct_csv_db():
    reg = sgbdSQL.consultar_db(
        "SELECT  distinct year,title,type as tipo,i.acronym as institution"
        + " from bibliographic_production AS b,  institution i, researcher r "
        + " where  "
        + "   r.id =  b.researcher_id "
        + "  AND r.institution_id = i.id "
        " GROUP BY year,title,tipo,i.acronym "
    )

    df_bd = pd.DataFrame(reg, columns=["year", "title", "tipo", "institution"])

    df_bd.to_csv(csv_dir + "production_year_distinct.csv")


def researcher_article_qualis_csv_db():
    sql = """
         	SELECT DISTINCT  title,
            bar.qualis as qualis,year,r.id as researcher_id,
            r.name as researcher_id,'institution' ,'city',
			pm.name AS name_magazine,pm.issn AS issn,bar.jcr as jcr,bar.jcr_link as jcr_link, b.type as type
          FROM  
          PUBLIC.bibliographic_production b LEFT JOIN  (bibliographic_production_article bar 
			 LEFT JOIN   periodical_magazine pm ON pm.id = bar.periodical_magazine_id) ON b.id = bar.bibliographic_production_id ,
	      researcher r
          WHERE 
           r.id =  b.researcher_id
         """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "title",
            "qualis",
            "year",
            "researcher_id",
            "researcher",
            "institution",
            "city",
            "name_magazine",
            "issn",
            "jcr",
            "jcr_link",
            "type",
        ],
    )

    df_bd.to_csv(csv_dir + "article_qualis_year.csv")


def article_qualis_csv_distinct_db():
    sql = """
                                SELECT distinct title,bar.qualis,bar.jcr,year,i.acronym as institution,rd.city as city,
                                       bar.jcr_link
                              
                                  FROM  PUBLIC.bibliographic_production b,bibliographic_production_article bar,
	                               periodical_magazine pm, researcher r, institution i, researcher_address rd 
                                    WHERE 
                                    pm.id = bar.periodical_magazine_id
                                 
                                    AND r.id =  b.researcher_id
                                    AND r.institution_id = i.id
    
                                    AND   b.id = bar.bibliographic_production_id
                                    AND rd.researcher_id = r.id
                                    group by title,bar.qualis,bar.jcr,year,i.acronym,rd.city,bar.jcr_link
                                    order by bar.qualis desc

        """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=["title", "qualis", "jcr", "year", "institution", "city", "jcr_link"],
    )
    df_bd.to_csv(csv_dir + "article_qualis_year_institution.csv")


def researcher_production_csv_db():
    sql = """
        SELECT r.name AS researcher, r.id AS researcher_id,
         rp.articles AS articles,
         rp.book_chapters AS book_chapters,
         rp.book AS book, rp.work_in_event AS work_in_event,
         rp.great_area AS great_area,
         rp.area_specialty AS area_specialty,
         r.graduation as graduation
        FROM researcher_production rp, researcher r 
          WHERE r.id= rp.researcher_id
   """
    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "researcher",
            "researcher_id",
            "articles",
            "book_chapters",
            "book",
            "work_in_event",
            "great_area",
            "area_specialty",
            "graduation",
        ],
    )

    df_bd.to_csv(csv_dir + "production__researcher.csv")


def researcher_csv_db():
    sql = """ SELECT r.name AS researcher, r.id AS researcher_id, TO_CHAR(r.last_update,'dd/mm/yyyy') date_,r.graduation as graduation,r.lattes_id
        

        FROM  researcher r """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "researcher",
            "researcher_id",
            "last_update",
            "graduation",
            "lattes_id",
        ],
    )

    df_bd.to_csv(csv_dir + "researcher.csv")


def institution_csv_db():
    sql = """ SELECT i.id, name, acronym 
        

        FROM  institution i """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(reg, columns=["institution_id", "name", "acronym"])

    df_bd.to_csv(csv_dir + "dim_institution.csv")


def save_data_to_csv():
    list_data = []
    hoje = str(datetime.now())
    data = {"data": hoje}
    list_data.append(data)

    json_string = json.dumps(list_data)

    json_buffer = StringIO(json_string)

    df = pd.read_json(json_buffer)

    df.to_csv(csv_dir + "data.csv")


def researcher_production_novo_csv_db():
    sql = """
            
         	SELECT title,
            
            
            qualis,year,r.id as researcher_id,
            r.name as researcher,
			bar.periodical_magazine_name as name_magazine,issn AS issn,jcr as jcr,jcr_link, b.type as type
                           
          FROM  
            PUBLIC.bibliographic_production b LEFT JOIN  bibliographic_production_article bar 
			  ON b.id = bar.bibliographic_production_id , researcher r
          WHERE 
          
         r.id =  b.researcher_id
      
         

                    
        
         """

    reg = sgbdSQL.consultar_db(sql)

    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "title",
            "qualis",
            "year",
            "researcher_id",
            "researcher",
            "name_magazine",
            "issn",
            "jcr",
            "jcr_link",
            "type",
        ],
    )

    df_bd.to_csv(csv_dir + "researcher_production_novo_csv_db.csv")


def article_distinct_novo_csv_db():
    sql = """
        SELECT 
            distinct title,
            bar.qualis,
            bar.jcr,
            b.year as year,
            gp.graduate_program_id as graduate_program_id,
            b.year as year_pos,
            periodical_magazine.name AS type
        FROM 
            bibliographic_production b 
            LEFT JOIN  bibliographic_production_article bar 
            ON b.id = bar.bibliographic_production_id 
            LEFT JOIN periodical_magazine ON periodical_magazine.id = bar.periodical_magazine_id, 
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id 
            AND gpr.researcher_id = r.id 
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
            AND b.type = 'ARTICLE'
            ------ AND gpr.type_ = 'PERMANENTE'
        order by qualis desc
   """

    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)
    df_bd = pd.DataFrame(
        reg,
        columns=[
            "title",
            "qualis",
            "jcr",
            "year",
            "graduate_program_id",
            "year_pos",
            "name_magazine",
        ],
    )

    df_bd.to_csv(csv_dir + "article_distinct_novo_csv_db.csv")


def production_coauthors_csv_db():
    sql = """
           SELECT COUNT(*),a.doi,a.title,ba.qualis,a.year,gp.graduate_program_id,gp.year,a."type"
		FROM bibliographic_production a LEFT JOIN bibliographic_production_article ba ON  a.id = ba.bibliographic_production_id, bibliographic_production b, 
		 graduate_program_researcher gp
      WHERE  (a.doi = b.doi OR a.title = b.title)    AND a.researcher_id = gp.researcher_id AND b.researcher_id = gp.researcher_id 
  
      GROUP BY a.doi,a.title,ba.qualis,a.year,gp.graduate_program_id,gp.year,a."type"
      HAVING COUNT(*)>1
    """

    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)
    df_bd = pd.DataFrame(
        reg,
        columns=[
            "qtd",
            "doi",
            "title",
            "qualis",
            "year",
            "graduate_program_id",
            "year_pos",
            "type",
        ],
    )

    df_bd.to_csv(csv_dir + "production_coauthors_csv_db.csv")


def production_distinct_novo_csv_db():
    sql = """
        SELECT 
            distinct title,
            qualis,jcr,
            b.year as year,
            gp.graduate_program_id as graduate_program_id,
            b.year as year_pos,
            b.type AS type 
        FROM 
            bibliographic_production b 
            LEFT JOIN  bibliographic_production_article bar 
            ON b.id = bar.bibliographic_production_id , 
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE 
            gpr.graduate_program_id = gp.graduate_program_id 
            AND gpr.researcher_id = r.id 
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
        order by qualis desc
    """

    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)
    df_bd = pd.DataFrame(
        reg,
        columns=[
            "title",
            "qualis",
            "jcr",
            "year",
            "graduate_program_id",
            "year_pos",
            "type",
        ],
    )

    df_bd.to_csv(csv_dir + "production_distinct_novo_csv_db.csv")


# Função processar e inserir a produção de cada pesquisador
def production_tecnical_year_novo_csv_db():
    sql = """
        SELECT DISTINCT
            title,
            development_year::INT AS YEAR,
            'PATENT' AS type,
            gp.graduate_program_id AS graduate_program_id,
            development_year::INT AS year_pos
        FROM
            patent p,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = p.researcher_id
            AND development_year::INT = ANY(gpr.year)
        UNION
        SELECT DISTINCT
            title,
            s.year AS YEAR,
            'SOFTWARE',
            gp.graduate_program_id AS graduate_program_id,
            s.year AS year_pos
        FROM
            software s,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = s.researcher_id
            AND s.year = ANY(gpr.year)
        UNION
        SELECT DISTINCT
            title,
            b.year AS YEAR,
            'BRAND',
            gp.graduate_program_id AS graduate_program_id,
            b.year AS year_pos
        FROM
            brand b,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = b.researcher_id
            AND b.year = ANY(gpr.year)
        UNION
        SELECT DISTINCT
            title,
            b.year AS YEAR,
            'REPORT',
            gp.graduate_program_id AS graduate_program_id,
            b.year AS year_pos
        FROM
            research_report b,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = b.researcher_id
            AND b.year = ANY(gpr.year)
        """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["title", "year", "type", "graduate_program_id", "year_pos"]
    )

    df_bd.to_csv(csv_dir + "production_tecnical_year_novo_csv_db.csv")


def graduate_program_researcher_csv_db():
    sql = """
    SELECT 
        researcher_id,
        graduate_program_id,
        2024,
        type_ 
    FROM graduate_program_researcher
    """
    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg, columns=["researcher_id", "graduate_program_id", "year", "type_"]
    )

    df_bd.to_csv(csv_dir + "cimatec_graduate_program_researcher.csv")


def graduate_program_student_researcher_csv_db():
    script_sql = """
        SELECT researcher_id, graduate_program_id, 2024
        FROM graduate_program_student
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_db = pd.DataFrame(
        registry, columns=["researcher_id", "graduate_program_id", "year"]
    )

    data_frame_db.to_csv(csv_dir + "cimatec_graduate_program_student.csv")


def profnit_graduate_program_csv_db():
    df_bd = graduate_programSQL.graduate_program_profnit_db()
    logger.debug(profnit_graduate_program_csv_db)

    df_bd.to_csv(csv_dir + "profnit_graduate_program.csv")


def graduate_program_csv_db():
    sql = """
        SELECT
            graduate_program_id,
            code,
            name,
            area,
            modality,
            type,
            rating,
            institution_id
        FROM 
            graduate_program gp
    """

    reg = sgbdSQL.consultar_db(sql)
    logger.debug(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "graduate_program_id",
            "code",
            "name",
            "area",
            "modality",
            "type",
            "rating",
            "institution_id",
        ],
    )

    df_bd.to_csv(csv_dir + "cimatec_graduate_program.csv")


def ind_prod_researcher_csv_db():
    script_sql = """
        SELECT
            researcher_id,
            YEAR,
            ind_prod_article,
            ind_prod_book,
            ind_prod_book_chapter,
            ind_prod_granted_patent,
            ind_prod_not_granted_patent,
            ind_prod_software,
            ind_prod_report,
            ind_prod_guidance
        FROM
            researcher_ind_prod;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_db = pd.DataFrame(
        registry,
        columns=[
            "researcher_id",
            "year",
            "ind_prod_article",
            "ind_prod_book",
            "ind_prod_book_chapter",
            "ind_prod_granted_patent",
            "ind_prod_not_granted_patent",
            "ind_prod_software",
            "ind_prod_report",
            "ind_prod_guidance",
        ],
    )

    data_frame_db.to_csv(
        csv_dir + "fat_researcher_ind_prod.csv",
        decimal=",",
        sep=";",
        index=False,
        encoding="UTF8",
        float_format=None,
    )


def graduate_program_researcher_year_unnest():
    script_sql = """
        SELECT
            graduate_program_id,
            researcher_id,
            unnest(year) AS year
        FROM
            public.graduate_program_researcher;
        """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=["graduate_program_id", "researcher_id", "year"])

    df.to_csv(csv_dir + "graduate_program_researcher_year_unnest.csv")


def dim_graduate_program_student_year_unnest():
    script_sql = """
        SELECT
            graduate_program_id,
            researcher_id,
            unnest(year) AS year
        FROM
            public.graduate_program_student;
        """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=["graduate_program_id", "researcher_id", "year"])

    df.to_csv(csv_dir + "graduate_program_student_year_unnest.csv")


def dim_graduate_program_acronym():
    script_sql = """
        SELECT
            graduate_program_id,
            acronym,
            name
        FROM 
            graduate_program
        """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        reg,
        columns=[
            "graduate_program_id",
            "acronym",
            "name",
        ],
    )

    df.to_csv(csv_dir + "dim_graduate_program_acronym.csv")


def graduate_program_ind_prod_csv_db():
    script_sql = """
        SELECT
            graduate_program_id,
            YEAR,
            ind_prod_article,
            ind_prod_book,
            ind_prod_book_chapter,
            ind_prod_granted_patent,
            ind_prod_not_granted_patent,
            ind_prod_software,
            ind_prod_report,
            ind_prod_guidance
        FROM
            graduate_program_ind_prod;
        """

    registry = sgbdSQL.consultar_db(script_sql)
    data_frame_db = pd.DataFrame(
        registry,
        columns=[
            "graduate_program_id",
            "year",
            "ind_prod_article",
            "ind_prod_book",
            "ind_prod_book_chapter",
            "ind_prod_granted_patent",
            "ind_prod_not_granted_patent",
            "ind_prod_software",
            "ind_prod_report",
            "ind_prod_guidance",
        ],
    )

    data_frame_db.to_csv(
        csv_dir + "graduate_program_ind_prod.csv",
        decimal=",",
        sep=";",
        index=False,
        encoding="UTF8",
        float_format=None,
    )


if __name__ == "__main__":
    csv_dir = "Files/indicadores_simcc/"
    log_dir = "Files/log"
    log_file = "logfile_csv.log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(
        filename=os.path.join(log_dir, log_file),
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger()
    logger.debug("Inicio")

    graduate_program_csv_db()
    graduate_program_researcher_csv_db()
    production_distinct_novo_csv_db()
    article_distinct_novo_csv_db()
    researcher_production_novo_csv_db()
    researcher_production_tecnical_year_csv_db()
    researcher_csv_db()
    graduate_program_ind_prod_csv_db()
    ind_prod_researcher_csv_db()
    production_coauthors_csv_db()
    researcher_production_year_csv_db()
    researcher_production_year_distinct_csv_db()
    researcher_article_qualis_csv_db()
    researcher_production_csv_db()
    article_qualis_csv_distinct_db()
    researcher_csv_db()
    researcher_production_tecnical_year_csv_db()
    institution_csv_db()
    fat_simcc_bibliographic_production()
    dim_researcher_csv_db()
    dim_institution_csv_db()
    dim_city_csv_db()
    fat_production_tecnical_year_novo_csv_db()
    fat_foment()
    dim_category_level_code()
    dim_research_group()
    fat_group_leaders()
    dim_departament_researcher()
    dim_departament_technician()
    graduate_program_researcher_year_unnest()
    dim_graduate_program_acronym()
    dim_graduate_program_student_year_unnest()
    graduate_program_student_researcher_csv_db()
    save_data_to_csv()
