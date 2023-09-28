import Dao.sgbdSQL as sgbdSQL
import Dao.graduate_programSQL as graduate_programSQL
import pandas as pd
import logging
import json
from datetime import datetime
import sys
#import lattes10 as lattes10


#dir="C:\\simccv3\\"
dir= host_=sys.argv[3]
print(dir)
# Função processar e inserir a produção de cada pesquisador
def researcher_production_tecnical_year_csv_db():
    
    
    sql= """
          SELECT researcher_id,title,development_year::int AS YEAR, 'PATENT' as type FROM patent
          UNION
          SELECT researcher_id,title,YEAR,'SOFTWARE' from software
          UNION
          SELECT researcher_id,title,YEAR,'BRAND' from brand
          UNION
          SELECT researcher_id,title,YEAR,'REPORT' from research_report

    """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=['researcher_id','title','year','type'])
    
    print(df_bd)
    logger.debug(sql)

    df_bd.to_csv(dir +'production_tecnical_year.csv')


# Função processar e inserir a produção de cada pesquisador
'''
def researcher_production_year_csv_db():
    
    
    reg = sgbdSQL.consultar_db("SELECT   title,b.type as tipo,b.researcher_id as researcher,year,i.acronym as institution, rd.city as city "+
                               " from bibliographic_production AS b, researcher r, institution i, researcher_address rd  "+
                     
                               " where b.researcher_id is not null "+
                               "   AND r.id =  b.researcher_id "+
                               "  AND r.institution_id = i.id "
                               " AND rd.researcher_id = r.id"
                         
                               " GROUP BY title,tipo,b.researcher_id,year,i.acronym, rd.city  ORDER  BY Tipo desc")

    df_bd = pd.DataFrame(reg, columns=['title','tipo','researcher','year','institution','city'])

    df_bd.to_csv('C:/simccv3/production_year.csv')
'''
''
def researcher_production_year_csv_db():

    
    
    reg = sgbdSQL.consultar_db("SELECT distinct  title,b.type as tipo,b.researcher_id,year,'institution' "+
                               " from bibliographic_production AS b, researcher r  "+
                     
                               " where b.researcher_id is not null "+
                               "   AND r.id =  b.researcher_id "+
                              
                             
                         
                               " GROUP BY title,tipo,b.researcher_id,year ORDER  BY year,Tipo desc")

    df_bd = pd.DataFrame(reg, columns=['title','tipo','researcher_id','year','institution'])

    df_bd.to_csv('C:/simccv3/production_year.csv')


def researcher_production_year_distinct_csv_db():
    
    
    reg = sgbdSQL.consultar_db("SELECT  distinct year,title,type as tipo,i.acronym as institution"+
                               " from bibliographic_production AS b,  institution i, researcher r "+
                     
                               " where  "+
                               "   r.id =  b.researcher_id "+
                               "  AND r.institution_id = i.id "
                               
                               " GROUP BY year,title,tipo,i.acronym ")

    df_bd = pd.DataFrame(reg, columns=['year','title','tipo','institution'])

    df_bd.to_csv('C:/simccv3/production_year_distinct.csv')

def researcher_article_qualis_csv_db():
   

   
   sql ="""
            
         	SELECT DISTINCT  title,
            
            
            qualis,year,r.id as researcher_id,
            r.name as researcher_id,'institution' ,'city',
			pm.name AS name_magazine,pm.issn AS issn,pm.jcr as jcr,jcr_link, b.type as type, 	CASE 
			WHEN qualis IS NULL THEN 'SQ'
			ELSE qualis
			END CASE 
                           
          FROM  
          PUBLIC.bibliographic_production b LEFT JOIN  (bibliographic_production_article bar 
			 LEFT JOIN   periodical_magazine pm ON pm.id = bar.periodical_magazine_id) ON b.id = bar.bibliographic_production_id ,
              
	      researcher r
          WHERE 
       
                                 
           r.id =  b.researcher_id
      
         

                    
        
         """


       

   reg = sgbdSQL.consultar_db(sql )
   
   df_bd = pd.DataFrame(reg, columns=['title','qualis','year','researcher_id','researcher','institution','city','name_magazine','issn','jcr','jcr_link','type','case'])


   df_bd.to_csv('C:/simccv3/article_qualis_year.csv')
   
def article_qualis_csv_distinct_db():
   
   sql= """
                                SELECT distinct title,qualis,jcr,year,i.acronym as institution,rd.city as city,
                                       jcr_link
                              
                                  FROM  PUBLIC.bibliographic_production b,bibliographic_production_article bar,
	                               periodical_magazine pm, researcher r, institution i, researcher_address rd 
                                    WHERE 
                                    pm.id = bar.periodical_magazine_id
                                 
                                    AND r.id =  b.researcher_id
                                    AND r.institution_id = i.id
    
                                    AND   b.id = bar.bibliographic_production_id
                                    AND rd.researcher_id = r.id
                                    group by title,qualis,jcr,year,i.acronym,rd.city,jcr_link
                                    order by qualis desc

        """

   reg = sgbdSQL.consultar_db( sql )
   
   df_bd = pd.DataFrame(reg, columns=['title','qualis','jcr','year','institution','city','jcr_link'])
   df_bd.to_csv('C:/simccv3/article_qualis_year_institution.csv')
   
def researcher_production_csv_db():
   sql="""
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
        
   
   df_bd = pd.DataFrame(reg, columns=['researcher','researcher_id','articles','book_chapters','book','work_in_event','great_area','area_specialty','graduation'])

   df_bd.to_csv('C:/simccv3/production__researcher.csv')


def researcher_csv_db():

   sql=""" SELECT r.name AS researcher, r.id AS researcher_id, TO_CHAR(r.last_update,'dd/mm/yyyy') date_,r.graduation as graduation
        

        FROM  researcher r """

   reg = sgbdSQL.consultar_db(sql)
   
   logger.debug(sql)
        
   
   df_bd = pd.DataFrame(reg, columns=['researcher','researcher_id','last_update','graduation'])

   df_bd.to_csv(dir+'researcher.csv')


def researcher_production_novo_csv_db():
   

   
   sql ="""
            
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


       

   reg = sgbdSQL.consultar_db(sql )

   logger.debug(sql)
   
   df_bd = pd.DataFrame(reg, columns=['title','qualis','year','researcher_id','researcher','name_magazine','issn','jcr','jcr_link','type'])


   df_bd.to_csv(dir+'researcher_production_novo_csv_db.csv')



   

def article_distinct_novo_csv_db():
   
   sql="""
         SELECT distinct title,qualis,jcr,b.year as year,gp.graduate_program_id as graduate_program_id,gpr.year as year_pos,bar.periodical_magazine_name 
                             
                                   FROM  PUBLIC.bibliographic_production b,bibliographic_production_article bar,
	                                researcher r, graduate_program_researcher gpr,  graduate_program gp
                                   WHERE 
                               
                                    gpr.graduate_program_id = gp.graduate_program_id 
                                   AND gpr.researcher_id = r.id 
                                     AND r.id = b.researcher_id
                                 
                                   AND   b.id = bar.bibliographic_production_id

                        order by qualis desc
   """

   reg = sgbdSQL.consultar_db(sql)   
   logger.debug(sql)
   df_bd = pd.DataFrame(reg, columns=[ 'title','qualis','jcr','year','graduate_program_id','year_pos','name_magazine'])
   
   df_bd.to_csv(dir+'article_distinct_novo_csv_db.csv') 

def production_distinct_novo_csv_db():
   
   sql="""
        SELECT distinct title,qualis,jcr,b.year as year,gp.graduate_program_id as graduate_program_id,gpr.year as year_pos,b.type AS type 
                             
                                   FROM  bibliographic_production b LEFT JOIN  bibliographic_production_article bar 
			 									 ON b.id = bar.bibliographic_production_id , 
	                                researcher r, graduate_program_researcher gpr,  graduate_program gp
                                   WHERE 
                               
                                    gpr.graduate_program_id = gp.graduate_program_id 
                                   AND gpr.researcher_id = r.id 
                                     AND r.id = b.researcher_id
                                 
                                   

                        order by qualis desc
    """

   reg = sgbdSQL.consultar_db(sql)   
   logger.debug(sql)
   df_bd = pd.DataFrame(reg, columns=[ 'title','qualis','jcr','year','graduate_program_id','year_pos','type'])
   
   df_bd.to_csv(dir+'production_distinct_novo_csv_db.csv') 
       

        
# Função processar e inserir a produção de cada pesquisador
def production_tecnical_year_novo_csv_db():
    
    
    sql= """
          SELECT distinct title,development_year::int AS year, 'PATENT' as type, gp.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          FROM patent p,graduate_program_researcher gpr,  graduate_program gp 
          WHERE  gpr.graduate_program_id = gp.graduate_program_id 
                AND gpr.researcher_id = p.researcher_id 

          UNION
          SELECT distinct title,s.year as year,'SOFTWARE',gp.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          from software s,graduate_program_researcher gpr,  graduate_program gp 
          WHERE  gpr.graduate_program_id = gp.graduate_program_id
                AND gpr.researcher_id = s.researcher_id 
          UNION
          SELECT distinct title,b.year as year,'BRAND',gp.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          from brand b,graduate_program_researcher gpr,  graduate_program gp 
          WHERE  gpr.graduate_program_id = gp.graduate_program_id 
                AND gpr.researcher_id = b.researcher_id 
          UNION
          SELECT distinct title,b.year as year,'REPORT',gp.graduate_program_id as graduate_program_id,gpr.year as year_pos 
          from research_report b,graduate_program_researcher gpr,  graduate_program gp 
          WHERE  gpr.graduate_program_id = gp.graduate_program_id 
                AND gpr.researcher_id = b.researcher_id 


               

    """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=['title','year','type','graduate_program_id','year_pos'])

    df_bd.to_csv(dir+'production_tecnical_year_novo_csv_db.csv')

def graduate_program_researcher_csv_db():
   sql="""
    SELECT researcher_id,graduate_program_id,year,type_ 
        FROM graduate_program_researcher
    """
   reg = sgbdSQL.consultar_db(sql )
   logger.debug(sql)
        
   
   df_bd = pd.DataFrame(reg, columns=[ 'researcher_id','graduate_program_id','year','type_'])

   df_bd.to_csv(dir+'cimatec_graduate_program_researcher.csv')     

def graduate_program_csv_db():

   sql="""
      SELECT graduate_program_id,code,name,area,modality,type,rating 
        FROM graduate_program gp
    """

   reg = sgbdSQL.consultar_db(sql )
   logger.debug(sql)

        
   
   df_bd = pd.DataFrame(reg, columns=[ 'graduate_program_id','code','name','area','modality','type','rating'])

   df_bd.to_csv(dir+'cimatec_graduate_program.csv')
  

def profnit_graduate_program_csv_db():

   df_bd = graduate_programSQL.graduate_program_profnit_db()
   logger.debug(profnit_graduate_program_csv_db)

   df_bd.to_csv(dir+'profnit_graduate_program.csv')


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "logfile_csv.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.DEBUG)

logger = logging.getLogger()

       
logger.debug("Inicio")
list_data=[]
hoje = str(datetime.now())
print(hoje)

data  = {
        'data': hoje
        
       

        }

list_data.append(data)
json_string = json.dumps(list_data)
df = pd.read_json(json_string)
df.to_csv(dir+'data.csv')

print("Inicio: graduate_program_csv_db")
graduate_program_csv_db()
print("Fim: graduate_program_csv_db")

print("Inicio: graduate_program_researcher_csv_db")
graduate_program_researcher_csv_db()   
print("Fim: graduate_program_researcher_csv_db")


print("Inicio: production_distinct_novo_csv_db")
production_distinct_novo_csv_db()
print("Fim: production_distinct_novo_csv_db")

print("Inicio: article_distinct_novo_csv_db")
article_distinct_novo_csv_db()
print("Fim: article_distinct_novo_csv_db")

print("Inicio: researcher_production_novo_csv_db")
researcher_production_novo_csv_db()
print("Fim: researcher_production_novo_csv_db")

print("Inicio: researcher_production_tecnical_year_csv_db")
researcher_production_tecnical_year_csv_db()
print("Fim: researcher_production_tecnical_year_csv_db")

if sys.argv[1]=="simcc_profnit_v1":
   profnit_graduate_program_csv_db()



print("Inicio: researcher_csv_db")
researcher_csv_db()
print("Fim: researcher_csv_db")



"""
researcher_production_year_csv_db()

researcher_production_year_distinct_csv_db()

researcher_article_qualis_csv_db()

researcher_production_csv_db()

article_qualis_csv_distinct_db()

researcher_csv_db()

researcher_production_tecnical_year_csv_db()
"""