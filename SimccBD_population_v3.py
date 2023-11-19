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
import logging
from datetime import datetime,timedelta
import lattes10
#import Soap_lattes


import concurrent.futures

import project as project_
dataP = datetime.today() - timedelta(days=100000)

import sys
project_.project_=sys.argv[1]
researcher_teste1 ="r.name LIKE \'Manoel %\' OR r.name LIKE \'Gesil Sampaio%\' "
#researcher_teste2 ="r.name LIKE \'Hugo Saba%\' OR r.name LIKE \'Josemar Rodri%\'"





    
    

def insert_researcher_frequency_db(teste,article,offset):
   time.sleep(3)
   filter=""
   if (teste==True):
      filter =  " and  b.created_at>= '%s'" % dataP
      #+ " or " +researcher_teste2
                  
   reg = sgbdSQL.consultar_db("SELECT  distinct  r.id from researcher r, bibliographic_production b where r.id =b.researcher_id "
                              +filter+" OFFSET "+str(offset) +" ROWS FETCH FIRST 100 ROW ONLY")
                    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
   df_bd = pd.DataFrame(reg, columns=['id'])
 
   for i,infos in df_bd.iterrows():
      researcher_id = infos.id
      print(infos.id)
      print((i+offset))
      logger.debug(sql)
      insert_researcher_frequency_caracter_bd(researcher_id,article)
      insert_researcher_abstract_frequency_caracter_bd(researcher_id)
      insert_researcher_patent_frequency_caracter_bd(researcher_id)
      logger.debug("Inserindo insert_researcher_frequency_db  "+str(researcher_id))
      

      

      
      #a = list(string.ascii_lowercase)
     
def insert_researcher_frequency_caracter_bd(researcher_id,article): 


  try:    
           

           
        sql = """
           
	         INSERT into public.researcher_frequency (term,researcher_id,bibliographic_production_id) 
	  
	  
	           SELECT   unaccent(r.term) as term,b.researcher_id as researcher_id ,b.id as bibliographic_production_id 
                                 FROM research_dictionary as r,bibliographic_production AS b   
                                 WHERE
                                  
                                (        translate(unaccent(LOWER(b.title)),':;''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              AND type='ARTICLE'
                                
                                 AND b.researcher_id='%s' AND r.type_='ARTICLE' 
        """ % (researcher_id)                                

        sgbdSQL.execScript_db(sql)
        #logger.debug(sql)
           
  except Exception as e: 
          print (e)         
          logger.error(e)
          traceback.print_exc()  
       
def insert_researcher_abstract_frequency_caracter_bd(researcher_id):     
     
        #print(caracter)
        try:
           
           sql= """
             INSERT into public.researcher_abstract_frequency (researcher_id,term) 
                 
                  SELECT  re.id, unaccent(r.term)
                                FROM research_dictionary as r,researcher as re
                                 WHERE 
                                (        translate(unaccent(LOWER(re.abstract)),':;\''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              
                             
                                  AND re.id= '%s' 
                                  AND r.type_='ABSTRACT' 
         
         """ % (researcher_id)

           sgbdSQL.execScript_db(sql)
         
        except Exception as e: 
          print (e)         
          traceback.print_exc()  


def insert_researcher_patent_frequency_caracter_bd(researcher_id):     
     
        #print(caracter)
        try:
           
           sql= """
             INSERT into public.researcher_patent_frequency (term,researcher_id,patent_id) 
	  
	  
	           SELECT   unaccent(r.term) as term,b.researcher_id as researcher_id ,b.id as patent_id 
                                 FROM research_dictionary as r,patent AS b   
                                 WHERE
                                  
                                (        translate(unaccent(LOWER(b.title)),':;''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              
                             
                                  AND b.researcher_id= '%s' 
                                  AND r.type_='PATENT' 
         
         """ % (researcher_id)

           sgbdSQL.execScript_db(sql)
         
        except Exception as e: 
          print (e)         
          traceback.print_exc()  



















# Função para consultar o Banco SIMCC
def create_researcher_dictionary_db(test,type):
 

  sql = "DELETE FROM research_dictionary "
  sgbdSQL.execScript_db(sql) 

  
  filter=""
  if (test==1):
      filter =  "  and b.created_at>= '%s'" % dataP
      #+ "  OR "+researcher_teste2
        
  sql="SELECT distinct  r.id from researcher r,bibliographic_production b where r.id=researcher_id " + filter 
  reg = sgbdSQL.consultar_db(sql)

  logger.debug(sql)
                 
  df_bd = pd.DataFrame(reg, columns=['id'])
  

  
  # Article
  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Article: "+str(i) )
        logger.debug("Total Pesquisador Article: "+str(i))
        
      
      create_researcher_title_dictionary_db(infos.id,type)   
     
 
  # Abstract
  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Abstract: "+str(i) )
        logger.debug("Total Pesquisador abstract: "+str(i)) 

      create_researcher_abstract_dictionary_db(infos.id)    

 #Patent
  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Patent: "+str(i) )
        logger.debug("Total Pesquisador Patent: "+str(i)) 

      create_researcher_patent_dictionary_db(infos.id)    
 



    




def create_researcher_title_dictionary_db(researcher_id,article):


  filter =""
  if article==1:
     filter =" AND  type='ARTICLE' AND created_at>= '%s'" % dataP
  reg = sgbdSQL.consultar_db('SELECT  distinct title,b.type,year from bibliographic_production AS b'+
  ' WHERE '+
 # is_new= true and 
  'researcher_id=\''+ researcher_id +"\'"+filter+
  # " AND char_length(unaccent(LOWER(r.term)))>3 AND to_tsvector('portuguese', unaccent(LOWER(r.term)))!='' and  unaccent(LOWER(r.term))!='sobre' "+
   # " AND (       translate(unaccent(LOWER(title)),':','') ::tsvector@@ '"+unidecode.unidecode("robotica | educacional | educacao")+"'::tsquery)=true"+
      #' AND "type" = \'ARTICLE\' '+
      #' AND b.title LIKE \'%ROBÓTICA%\' '+
      ' GROUP BY title,b.type,year')
  logger.debug("Entrei nos artigos pesquisador "+researcher_id)
  df_bd = pd.DataFrame(reg, columns=['title','b.type','year'])
    #print(df_bd.head())
  texto ="" 
  
  for i,infos in df_bd.iterrows():
      #print(infos.title)
      #print(infos.year)
      #Retirando a pontuação
      tokenize = RegexpTokenizer(r'\w+')
      tokens =[]
      tokens = tokenize.tokenize(infos.title)   
      #print(infos.title) 
      
      insert_research_dictionary_db(tokens,"ARTICLE")


  
def create_researcher_patent_dictionary_db(researcher_id):


  filter =""
  if article==1:
     filter =" and created_at>= '%s'" % dataP
  reg = sgbdSQL.consultar_db('SELECT  distinct title,development_year  as  year  from patent AS b'+
  ' WHERE '+
 # is_new= true and 
  'researcher_id=\''+ researcher_id +"\'"+filter+
  # " AND char_length(unaccent(LOWER(r.term)))>3 AND to_tsvector('portuguese', unaccent(LOWER(r.term)))!='' and  unaccent(LOWER(r.term))!='sobre' "+
   # " AND (       translate(unaccent(LOWER(title)),':','') ::tsvector@@ '"+unidecode.unidecode("robotica | educacional | educacao")+"'::tsquery)=true"+
      #' AND "type" = \'ARTICLE\' '+
      #' AND b.title LIKE \'%ROBÓTICA%\' '+
      ' GROUP BY  title,development_year ')
  logger.debug("Entrei nas patentes  "+researcher_id)
  df_bd = pd.DataFrame(reg, columns=['title','year'])
    #print(df_bd.head())
  texto ="" 
  
  for i,infos in df_bd.iterrows():
      #print(infos.title)
      #print(infos.year)
      #Retirando a pontuação
      tokenize = RegexpTokenizer(r'\w+')
      tokens =[]
      tokens = tokenize.tokenize(infos.title)   
      #print(infos.title) 
      
      insert_research_dictionary_db(tokens,"PATENT")

def create_researcher_abstract_dictionary_db(researcher_id):


 
  reg = sgbdSQL.consultar_db('SELECT  r.abstract as abstract from researcher as r'+
  ' WHERE ' +
   # update_abstract=true and
   'r.id=\''+ researcher_id +"\'")
  
  df_bd = pd.DataFrame(reg, columns=['abstract'])
    #print(df_bd.head())
  texto ="" 
  x=0;
  for i,infos in df_bd.iterrows():
      #print(infos.title)
      #print(infos.year)
      #Retirando a pontuação
      tokenize = RegexpTokenizer(r'\w+')
      tokens =[]
     
      #print(infos.abstract)
      if (infos.abstract is not None):
        tokens = tokenize.tokenize(infos.abstract)   
      #print(infos.title) 
      
        insert_research_dictionary_db(tokens,"ABSTRACT")     
  
  i1 =0      

  


def insert_research_dictionary_db(tokens,type):

      stopwords_portuguese = nltk.corpus.stopwords.words('portuguese')
      stopwords_english = nltk.corpus.stopwords.words('english')

      word_previous = ""
      for word in tokens:
        if len(word)>=3:
          if not((word.lower() in stopwords_portuguese) or (word.lower() in stopwords_english)):
                
                #sql="SELECT count(*) as total FROM research_dictionary WHERE type_='%s' AND term='%s'" % (type,word.lower()) 
                #reg = sgbdSQL.consultar_db(sql)               
                #df_bd = pd.DataFrame(reg, columns=['total'])
  
                #for i,infos in df_bd.iterrows():
                    #if (infos.total==0): 
                    try:
            
                   
                      sql = """
                      INSERT into public.research_dictionary  (term,frequency,type_)  VALUES ('%s',1,'%s') 
                      ON CONFLICT (term,type_) 
                      DO 
                      UPDATE  SET  frequency=(select frequency from research_dictionary WHERE term= EXCLUDED.term AND type_=EXCLUDED.type_) +1 ;
     			            """ % (word.lower(),type)

                      sgbdSQL.execScript_db(sql)

                    #else:  
                    except Exception as e: 
                       
                       print(e)
                       logger.debug(e)

                    
                 

    






Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "logfile_Population.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.DEBUG)

logger = logging.getLogger()

       
logger.debug("Inicio")
#try:

  #lattes10.lattes_10_researcher_frequency_db(logger)
#except Exception as e: 
 #         print (e)         
 #         traceback.print_exc()   

sql="""

UPDATE bibliographic_production_article ba SET qualis='A4' WHERE ba.issn='17412242'

"""
sgbdSQL.execScript_db(sql)
logger.debug(sql)

sql = """

UPDATE  bibliographic_production_article p SET jcr=(subquery.jif2019),jcr_link=url_revista
FROM (SELECT jif2019,eissn,url_revista
      FROM  "JCR_novo_link_v1" ) AS subquery
WHERE translate(subquery.eissn,'-','')=p.issn
"""
sgbdSQL.execScript_db(sql)

logger.debug(sql)

sql = """

UPDATE  bibliographic_production_article p SET jcr=(subquery.jif2019),jcr_link=url_revista
FROM (SELECT jif2019,issn,url_revista
      FROM  "JCR_novo_link_v1" ) AS subquery
WHERE translate(subquery.issn,'-','')=p.issn
"""
sgbdSQL.execScript_db(sql)

logger.debug(sql)


#print(areaFlowSQL.lists_area_speciality_researcher_db('215a5c60-d882-4936-9445-da4742c14802'))
sql = """
      UPDATE bibliographic_production SET YEAR_=YEAR::INTEGER
        """ 

sgbdSQL.execScript_db(sql)
logger.debug(sql)



sql = """ UPDATE bibliographic_production_article  SET qualis='B2' WHERE issn='26748568' OR issn='2764622'"""

sgbdSQL.execScript_db(sql)

logger.debug(sql)


print("Passo II")


#create_researcher_production_db(0 )









#create_area_ditionary_db()
teste=True
article=True
create_researcher_dictionary_db(teste,article)



#Levenshtein Distance

sql = "DELETE FROM researcher_frequency "
sgbdSQL.execScript_db(sql)            
sql = "DELETE FROM researcher_patent_frequency "
sgbdSQL.execScript_db(sql) 
sql = "DELETE FROM researcher_abstract_frequency "
sgbdSQL.execScript_db(sql)            
#insert_researcher_frequency_bigrama_db(1)
for i in range(4928):
  if (i%100)==0:
      print(i)
      t=threading.Thread(target=insert_researcher_frequency_db,args=(teste,article,i), name='t'+str(i))
      t.start()
      print("Thread ativa %x " % threading.active_count())
      #t.start()

      #insert_researcher_frequency_db(1)




#lista = {}
#lista ['Robótica'] = {"frequencia":1}
#lista ['Educacao'] = {"frequencia":0}

#print(lista['Robótica'].get("termo"))
#print(lista['Robótica'].get("frequencia"))
#lista['Robótica'].update( {"frequencia":12})
#print(lista['Robótica'].get("frequencia"))




