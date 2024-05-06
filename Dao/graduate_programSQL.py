import pandas as pd

import Dao.sgbdSQL as sgbdSQL
import Model.GraduateProgram_Production as GraduateProgram_Production


def graduate_program_db(institution_id):

    reg = sgbdSQL.consultar_db(
        " SELECT graduate_program_id,code,name as program,area,modality,type,rating "
        " FROM graduate_program gp where institution_id='%s'" % institution_id
    )

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "graduate_program_id",
            "code",
            "program",
            "area",
            "modality",
            "type",
            "rating",
        ],
    )

    print(df_bd)

    return df_bd


def graduate_program_profnit_db(graduate_program_id):

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = (
            f"WHERE gp.graduate_program_id = '{graduate_program_id}';"
        )

    script_sql = f"""
        SELECT 
            gp.graduate_program_id,
            gp.code,
            gp.name AS program,
            gp.area,
            gp.modality,
            gp.type,
            gp.rating,
            gp.state,
            gp.city,
            gp.instituicao,
            gp.url_image,
            gp.region,
            gp.sigla,
            gp.visible
        FROM 
            graduate_program gp
        {filter_graduate_program}
      """
    print(script_sql)

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "graduate_program_id",
            "code",
            "program",
            "area",
            "modality",
            "type",
            "rating",
            "state",
            "city",
            "instituicao",
            "url_image",
            "region",
            "sigla",
            "visible",
        ],
    )

    return data_frame


def production_general_db(graduate_program_id, year):

    filter = ""

    if graduate_program_id != "0":
        filter = f"AND graduate_program_id = '{graduate_program_id}'"

    if filter != "":
        sql = f"""
            SELECT COUNT(graduate_program_id) AS qtd, 
                   'PATENT' AS type, 
                   graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM patent p, graduate_program_researcher gpr
            WHERE gpr.researcher_id = p.researcher_id {filter} AND p.development_year::int >= {year}
            GROUP BY type, graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'SOFTWARE' AS type, 
                   graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM software s, graduate_program_researcher gpr
            WHERE gpr.researcher_id = s.researcher_id {filter} AND s.year >= {year}
            GROUP BY graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'BRAND' AS type, 
                   graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM brand b, graduate_program_researcher gpr
            WHERE gpr.researcher_id = b.researcher_id {filter} AND b.year >= {year}
            GROUP BY graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'ARTICLE' AS type, 
                   gpr.graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM PUBLIC.bibliographic_production b, graduate_program_researcher gpr
            WHERE b.researcher_id = gpr.researcher_id AND TYPE = 'ARTICLE' {filter} AND year_ >= {year}
            GROUP BY TYPE, graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'BOOK' AS type, 
                   gpr.graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM PUBLIC.bibliographic_production b, graduate_program_researcher gpr
            WHERE b.researcher_id = gpr.researcher_id AND TYPE = 'BOOK' {filter} AND year_ >= {year}
            GROUP BY TYPE, graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'BOOK_CHAPTER' AS type, 
                   gpr.graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM PUBLIC.bibliographic_production b, graduate_program_researcher gpr
            WHERE b.researcher_id = gpr.researcher_id AND TYPE = 'BOOK_CHAPTER' {filter} AND year_ >= {year}
            GROUP BY TYPE, graduate_program_id, gpr.year 

            UNION

            SELECT COUNT(graduate_program_id) AS qtd, 
                   'WORK_IN_EVENT' AS type, 
                   gpr.graduate_program_id AS graduate_program_id, 
                   gpr.year AS year_pos 
            FROM PUBLIC.bibliographic_production b, graduate_program_researcher gpr
            WHERE b.researcher_id = gpr.researcher_id AND TYPE = 'WORK_IN_EVENT' {filter} AND year_ >= {year}
            GROUP BY TYPE, graduate_program_id, gpr.year
            """
    else:
        sql = f"""
            SELECT COUNT(r.id) AS qtd, 'PATENT' AS type
            FROM patent p, researcher r
            WHERE r.id = p.researcher_id AND p.development_year::int >= {year}
            GROUP BY type
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'SOFTWARE' AS TYPE
            FROM software s, researcher r
            WHERE r.id = s.researcher_id AND s.year >= {year}
            GROUP BY TYPE
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'BRAND' AS TYPE
            FROM brand b, researcher r
            WHERE r.id = b.researcher_id AND b.year >= {year}
            GROUP BY TYPE
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'ARTICLE' AS TYPE
            FROM PUBLIC.bibliographic_production b, researcher r
            WHERE b.researcher_id = r.id AND TYPE = 'ARTICLE' AND year_ >= {year}
            GROUP BY TYPE
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'BOOK' AS TYPE
            FROM PUBLIC.bibliographic_production b, researcher r
            WHERE b.researcher_id = r.id AND TYPE = 'BOOK' AND year_ >= {year}
            GROUP BY TYPE
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'BOOK_CHAPTER' AS TYPE
            FROM PUBLIC.bibliographic_production b, researcher r
            WHERE b.researcher_id = r.id AND TYPE = 'BOOK_CHAPTER' AND year_ >= {year}
            GROUP BY TYPE
            
            UNION
            
            SELECT COUNT(r.id) AS qtd, 'WORK_IN_EVENT' AS TYPE
            FROM PUBLIC.bibliographic_production b, researcher r
            WHERE b.researcher_id = r.id AND TYPE = 'WORK_IN_EVENT' AND year_ >= {year}
            GROUP BY TYPE
            """

    reg = sgbdSQL.consultar_db(sql)

    if filter != "":
        df_bd = pd.DataFrame(
            reg, columns=["qtd", "tipo", "graduate_program_id", "year"]
        )
    else:
        df_bd = pd.DataFrame(reg, columns=["qtd", "tipo"])

    list_graduateProgram_Production = []
    graduateProgram_Production_ = (
        GraduateProgram_Production.GraduateProgram_Production()
    )
    graduateProgram_Production_.id = graduate_program_id
    for i, infos in df_bd.iterrows():

        # print(infos.tipo)
        # print(infos.qtd)

        if infos.tipo == "BOOK":

            graduateProgram_Production_.book = infos.qtd

        if infos.tipo == "WORK_IN_EVENT":
            graduateProgram_Production_.work_in_event = infos.qtd

        if infos.tipo == "ARTICLE":
            graduateProgram_Production_.article = infos.qtd

        if infos.tipo == "BOOK_CHAPTER":
            graduateProgram_Production_.book_chapter = infos.qtd
        if infos.tipo == "PATENT":
            graduateProgram_Production_.patent = infos.qtd
        if infos.tipo == "SOFTWARE":
            graduateProgram_Production_.software = infos.qtd
        if infos.tipo == "BRAND":
            graduateProgram_Production_.brand = infos.qtd

    if filter != "":
        sql = f"""
            SELECT COUNT(*) AS qtd 
            FROM graduate_program_researcher gpr 
            WHERE graduate_program_id = '{graduate_program_id}'
            """
    else:
        sql = """
            SELECT COUNT(*) AS qtd 
            FROM researcher r"""

    reg = sgbdSQL.consultar_db(sql)
    df_bd = pd.DataFrame(reg, columns=["qtd"])
    for i, infos in df_bd.iterrows():

        graduateProgram_Production_.researcher = str(infos.qtd)

    list_graduateProgram_Production.append(graduateProgram_Production_.getJson())

    return list_graduateProgram_Production
