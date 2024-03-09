import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util
import Model.GraduateProgram_Production as GraduateProgram_Production

# Função para listar a palavras do dicionário passando as iniciais

"""
    Consulta um banco de dados para recuperar informações sobre programas de pós-graduação de uma instituição específica.

    Args:
        institution_id (str): O ID da instituição para a qual deseja recuperar os programas de pós-graduação.

    Returns:
        pd.DataFrame: Um DataFrame do pandas contendo informações sobre os programas de pós-graduação da instituição.
    """


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
            gp.sigla
        FROM 
            graduate_program gp
        {filter_graduate_program}
      """

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
            "sigla"
        ],
    )

    return data_frame


def production_general_db(graduate_program_id, year):

    filter = ""

    if graduate_program_id != "0":
        filter = "and graduate_program_id=" + graduate_program_id

    if filter != "":
        sql = """
            SELECT COUNT(graduate_program_id) as qtd, 'PATENT' as type, graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM patent p,graduate_program_researcher gpr
          WHERE  gpr.researcher_id = p.researcher_id %s and p.development_year::int  >=%s
                group by  type,graduate_program_id,gpr.year 
                 UNION
          SELECT COUNT(graduate_program_id) as qtd,'SOFTWARE' as type,graduate_program_id as graduate_program_id,gpr.year as year_pos 
          from software s,graduate_program_researcher gpr
          WHERE  gpr.researcher_id = s.researcher_id %s and  s.year >=%s
                 group by  graduate_program_id ,gpr.year 
                 
       UNION
          SELECT COUNT(graduate_program_id) as qtd ,'BRAND' as type,graduate_program_id as graduate_program_id,gpr.year as year_pos 
          from brand b,graduate_program_researcher gpr
          WHERE  
                 gpr.researcher_id = b.researcher_id %s and b.year >=%s
                
                 group by   graduate_program_id ,gpr.year 
                  
      UNION                 
  
      SELECT COUNT(graduate_program_id) as qtd,'ARTICLE' as type,gpr.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM   PUBLIC.bibliographic_production b , graduate_program_researcher gpr
			 where  b.researcher_id =gpr.researcher_id AND TYPE='ARTICLE'  %s and year_ >=%s
          
                
                 group BY  TYPE, graduate_program_id ,gpr.year 
                 
      UNION
	   SELECT COUNT(graduate_program_id) as qtd,'BOOK' as type ,gpr.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM   PUBLIC.bibliographic_production b , graduate_program_researcher gpr
			 where  b.researcher_id =gpr.researcher_id AND TYPE='BOOK' %s and year_ >=%s
          
                
                 group BY TYPE, graduate_program_id ,gpr.year               
                 
      UNION
	  SELECT COUNT(graduate_program_id) as qtd,'BOOK_CHAPTER' as type,gpr.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM   PUBLIC.bibliographic_production b , graduate_program_researcher gpr
			 where  b.researcher_id =gpr.researcher_id AND TYPE='BOOK_CHAPTER' %s and year_ >=%s
          
                
                 group BY TYPE, graduate_program_id ,gpr.year         
	  UNION				   
					                 
                 	SELECT COUNT(graduate_program_id) as qtd,'WORK_IN_EVENT' as type,gpr.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM   PUBLIC.bibliographic_production b , graduate_program_researcher gpr 
			 where  b.researcher_id =gpr.researcher_id AND TYPE='WORK_IN_EVENT' %s and year_ >=%s
          
                
                 group BY  TYPE, graduate_program_id ,gpr.year    

               

       """ % (
            filter,
            year,
            filter,
            year,
            filter,
            year,
            filter,
            year,
            filter,
            year,
            filter,
            year,
            filter,
            year,
        )
    else:
        sql = """
            
         SELECT COUNT(r.id) as qtd, 'PATENT' as type
          FROM patent p,researcher r
          WHERE  r.id = p.researcher_id  and p.development_year::int  >=%s
                group by  type
                 UNION
          SELECT COUNT(r.id) as qtd,'SOFTWARE' as TYPE 
          from software s,researcher r
          WHERE  r.id = s.researcher_id  and  s.year >=%s
                 group by  TYPE

       UNION
          SELECT COUNT(r.id) as qtd ,'BRAND' as TYPE
          from brand b,researcher r
          WHERE
                 r.id = b.researcher_id  and b.year >=%s

                 group by  TYPE

      UNION

      SELECT COUNT(r.id) as qtd,'ARTICLE' as TYPE
          FROM   PUBLIC.bibliographic_production b , researcher r
                         where  b.researcher_id =r.id AND TYPE='ARTICLE'   and year_ >=%s


                 group BY  TYPE

      UNION
           SELECT COUNT(r.id) as qtd,'BOOK' as TYPE 
          FROM   PUBLIC.bibliographic_production b , researcher r
                         where  b.researcher_id =r.id AND TYPE='BOOK'  and year_ >=%s


                 group BY TYPE
     UNION
          SELECT COUNT(r.id) as qtd,'BOOK_CHAPTER' as TYPE
          FROM   PUBLIC.bibliographic_production b ,researcher r
                         where  b.researcher_id =r.id AND TYPE='BOOK_CHAPTER'  and year_ >=%s


                 group BY TYPE
          UNION

                        SELECT COUNT(r.id) as qtd,'WORK_IN_EVENT' as TYPE
          FROM   PUBLIC.bibliographic_production b , researcher r
                         where  b.researcher_id =r.id AND TYPE='WORK_IN_EVENT'  and year_ >=%s


                 group BY  TYPE

           """ % (
            year,
            year,
            year,
            year,
            year,
            year,
            year,
        )
    print(sql)
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
        sql = """
        select count(*) as qtd from graduate_program_researcher gpr where graduate_program_id=%s
       """ % (
            graduate_program_id
        )
    else:
        sql = """
       select count(*) as qtd from researcher r """

    reg = sgbdSQL.consultar_db(sql)
    df_bd = pd.DataFrame(reg, columns=["qtd"])
    for i, infos in df_bd.iterrows():

        graduateProgram_Production_.researcher = str(infos.qtd)

    list_graduateProgram_Production.append(
        graduateProgram_Production_.getJson())

    return list_graduateProgram_Production
