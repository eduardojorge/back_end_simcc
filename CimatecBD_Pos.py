import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import psycopg2
import nltk
from nltk.tokenize import RegexpTokenizer
import unidecode
import string
import threading
import time
import traceback
import Dao.areaFlowSQL as areaFlowSQL
import Dao.util as util
import Dao.termFlowSQL as termFlowSQL
import project as project_
import sys
dir= host_=sys.argv[2]
project_.project_=sys.argv[1]


def insert_researcher_graduate_program_db(table,graduate_program_id,year):
   




   reg = sgbdSQL.consultar_db(

                                "SELECT  r.id,gdp.type_ from "+table+" gdp, researcher r "
                                " where similarity(unaccent(LOWER(gdp.name)),unaccent(LOWER(r.name)))>0.8 ")
              
   df_bd = pd.DataFrame(reg, columns=['id','type_'])
   m=[];
   for i,infos in df_bd.iterrows():
        researcher_id = infos.id
        type_= infos.type_
        sql = """INSERT into public.graduate_program_researcher (researcher_id,graduate_program_id,year,type_) 
                values('%s','%s','%s','%s');""" % (researcher_id,graduate_program_id,year,type_)

        sgbdSQL.execScript_db(sql)

def graduate_program_csv_db():

   reg = sgbdSQL.consultar_db( " SELECT graduate_program_id,code,name,area,modality,type,rating "
       " FROM graduate_program gp")
        
   
   df_bd = pd.DataFrame(reg, columns=[ 'graduate_program_id','code','name','area','modality','type','rating'])

   df_bd.to_csv('C:/simccv3/cimatec_graduate_program.csv')

def graduate_program_researcher_csv_db():

   reg = sgbdSQL.consultar_db( " SELECT researcher_id,graduate_program_id,year,type_ "
       " FROM graduate_program_researcher")
        
   
   df_bd = pd.DataFrame(reg, columns=[ 'researcher_id','graduate_program_id','year','type_'])

   df_bd.to_csv('C:/simccv3/cimatec_graduate_program_researcher.csv') 

def graduate_program_student_researcher_csv_db():

   reg = sgbdSQL.consultar_db( " SELECT lattes_id,graduate_program_id,year,type_ "
       " FROM graduate_program_student")
        
   
   df_bd = pd.DataFrame(reg, columns=[ 'lattes_id','graduate_program_id','year','type_'])

   df_bd.to_csv('C:/simccv3/cimatec_graduate_program_student.csv')    


def cimatec_researcher_production_year_distinct_csv_db():
    
    sql= """
          SELECT   distinct title,b.type as tipo,b.year as year,
          gp.graduate_program_id as graduate_program_id,gpr.year as year_pos,bpa.qualis AS qualis
          from bibliographic_production AS b LEFT JOIN  bibliographic_production_article AS bpa ON b.id = bpa.bibliographic_production_id 
          LEFT JOIN  periodical_magazine as pm ON  bpa.periodical_magazine_id =  pm.id
          , researcher r, institution i,graduate_program_researcher gpr,  graduate_program gp

          where  gpr.graduate_program_id = gp.graduate_program_id 
         AND gpr.researcher_id = r.id 
         AND r.id =  b.researcher_id 
         AND r.institution_id = i.id 
                                 
         """
    
    '''
    reg = sgbdSQL.consultar_db("SELECT   distinct title,b.type as tipo,b.year as year,gp.graduate_program_id as graduate_program_id,gpr.year as year_pos"+
                               " from bibliographic_production AS b, researcher r, institution i,graduate_program_researcher gpr,  graduate_program gp  "+
                     
                               " where  gpr.graduate_program_id = gp.graduate_program_id "
                                  "  AND gpr.researcher_id = r.id "
                               "   AND r.id =  b.researcher_id "+
                               "  AND r.institution_id = i.id ")
                             
    '''       
    reg = sgbdSQL.consultar_db(sql)         

                            

    df_bd = pd.DataFrame(reg, columns=['title','tipo','year','graduate_program_id','year_pos','qualis'])

    df_bd.to_csv('C:/simccv3/cimatec_production_year_distinct.csv')   

def cimatec_article_qualis_distinct_csv_db():

   reg = sgbdSQL.consultar_db(  " SELECT distinct title,bar.qualis,bar.jcr,b.year as year,gp.graduate_program_id as graduate_program_id,gpr.year as year_pos "+
                              "   "
                                  " FROM  PUBLIC.bibliographic_production b,bibliographic_production_article bar,"+
	                               "periodical_magazine pm, researcher r, graduate_program_researcher gpr,  graduate_program gp" + 
                                  "  WHERE "+
                                  "  pm.id = bar.periodical_magazine_id"+
                                  "  AND gpr.graduate_program_id = gp.graduate_program_id "
                                  "  AND gpr.researcher_id = r.id "
                                  "   AND r.id = b.researcher_id"
                                 
                                  " AND   b.id = bar.bibliographic_production_id"+
                                   
       

                    


                        "  group by title,bar.qualis,bar.jcr,b.year,gp.graduate_program_id, gpr.year   order by qualis desc")   
   df_bd = pd.DataFrame(reg, columns=[ 'title','qualis','jcr','year','graduate_program_id','year_pos'])
   
   df_bd.to_csv('C:/simccv3/cimatec_article_qualis_distinct.csv') 


def insert_student_graduate_program_db(table,graduate_program_id,year):
   




   reg = sgbdSQL.consultar_db(

                                "SELECT  r.id from "+table+" gdp, researcher r "
                                " where similarity(unaccent(LOWER(gdp.aluno)),unaccent(LOWER(r.name)))>0.8 ")
              
   df_bd = pd.DataFrame(reg, columns=['id'])
   m=[];
   for i,infos in df_bd.iterrows():
        researcher_id = infos.id
        type_= "EFETIVO"
        sql = """INSERT into public.graduate_program_student (researcher_id,graduate_program_id,year,type_) 
                values('%s','%s','%s','%s');""" % (researcher_id,graduate_program_id,year,type_)

        sgbdSQL.execScript_db(sql)


        
# Função processar e inserir a produção de cada pesquisador
def cimatec_production_tecnical_year_csv_db():
    
    
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

    df_bd.to_csv('C:/simccv3/cimatec_production_tecnical_year.csv')

#insert_student_graduate_program_db("aluno_getec_dou",4,2023)

'''
insert_researcher_graduate_program_db("cimatec_getec_doutorado_pos_2023_",4,2023)

insert_researcher_graduate_program_db("cimatec_getec_mestrado_pos_2023",3,2023)

insert_researcher_graduate_program_db("cimatec_mcti_pos_2023",5,2023)

insert_researcher_graduate_program_db("cimatec_mpds_pos_2023",1,2023)
'''

graduate_program_csv_db()
graduate_program_researcher_csv_db()
graduate_program_student_researcher_csv_db()
graduate_program_researcher_csv_db()
cimatec_article_qualis_distinct_csv_db()
cimatec_researcher_production_year_distinct_csv_db()
cimatec_production_tecnical_year_csv_db()




 
     
