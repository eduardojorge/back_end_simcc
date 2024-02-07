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


list_word = {}
lista_word_bigram ={}

list_word_area = {}
lista_word_area_bigram ={}


    
    

def insert_researcher_frequency_db(teste,article,offset):
   time.sleep(3)
   filter=""
   if (teste==True):
      filter =  " and  b.created_at>= '%s'" % dataP
      #+ " or " +researcher_teste2
                  
   reg = sgbdSQL.consultar_db("SELECT   r.id from researcher r, bibliographic_production b where r.id =b.researcher_id "+filter+" OFFSET "+str(offset) +" ROWS FETCH FIRST 100 ROW ONLY")
                    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
   df_bd = pd.DataFrame(reg, columns=['id'])
   m=[];
   for i,infos in df_bd.iterrows():
      researcher_id = infos.id
      print(infos.id)
      print((i+offset))
      insert_researcher_frequency_caracter_bd(researcher_id,article)
      insert_researcher_abstract_frequency_caracter_bd(researcher_id)

      

      
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
        logger.debug(sql)
           
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
                                FROM research_dictionary as r,researcher re
                                 WHERE 
                                (        translate(unaccent(LOWER(re.abstract)),':;\''','') ::tsvector@@ unaccent(LOWER(r.term))::tsquery)=TRUE 
                                
                              
                             
                                  AND re.id= '%s' 
                                  AND r.type_='ABSTRACT' 
         
         """ % (researcher_id)

           sgbdSQL.execScript_db(sql)
         
        except Exception as e: 
          print (e)         
          traceback.print_exc()  
























def insert_researcher_frequency_bigrama_db(teste,article):
   filter=""
   if (teste==1):
      filter =  " where id=\'35e6c140-7fbb-4298-b301-c5348725c467\'  OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\'"
                  
   reg = sgbdSQL.consultar_db('SELECT   id from researcher '+filter)
                   
   df_bd = pd.DataFrame(reg, columns=['id'])
   m=[];
   for i,infos in df_bd.iterrows():
      researcher_id = infos.id
      print(infos.id)
      reg = sgbdSQL.consultar_db("SELECT  term_1,term_2  FROM research_dictionary_bigram WHERE term_1 <> term_2 ORDER BY frequency desc fetch FIRST 200 rows only")
                                 #"AND term_1 LIKE 'rob%' or term_1 LIKE 'edu%'")
                        
      df_bd = pd.DataFrame(reg, columns=['term_1','term_2'])
      list=[]
      for i,infos in df_bd.iterrows():
          if(infos.term_1 != infos.term_2):
            term_1 = infos.term_1.replace(' ',' & ')
            term_2 = infos.term_2.replace(' ',' & ')
          
            term = term_1 + ' & '+ term_2


            print(term)  
            filter=""
            if article==1:
              filter =" AND type='ARTICLE' "
            reg = sgbdSQL.consultar_db('SELECT b.id as id from bibliographic_production AS b'+
            ' WHERE '+
          
            '  AND researcher_id=\''+  researcher_id +"\'"+
              filter+
            " AND (       translate(unaccent(LOWER(b.title)),':','') ::tsvector@@ '"+unidecode.unidecode(term)+"'::tsquery)=true")

           
            df_bd = pd.DataFrame(reg, columns=['id'])
            for i,infos in df_bd.iterrows():
          
              bibliographic_production_id = infos.id

              sql = """INSERT into public.researcher_frequency (researcher_id,bibliographic_production_id,term) 
                values('%s','%s','%s');""" % (researcher_id,bibliographic_production_id,term_1+' '+term_2)

              sgbdSQL.execScript_db(sql)
def create_area_ditionary_db():

  sql = "DELETE FROM public.area_dictionary "
  sgbdSQL.execScript_db(sql)

  sql = "DELETE FROM public.area_dictionary_bigram  "
  sgbdSQL.execScript_db(sql)
   
  reg = sgbdSQL.consultar_db("SELECT a.name as term_1,b.name as term_2   from area_expertise a,  area_expertise b "+

                       " WHERE a.id = b.parent_id "+
                       " AND a.name LIKE 'His%' "
                       # " or a.name LIKE 'edu%'"+
                       " AND a.parent_id IS NOT NULL ")
                        
  df_bd = pd.DataFrame(reg, columns=['term_1','term_2'])
  tokens =[]
  for i,infos in df_bd.iterrows():
      #print(infos.title)
      #print(infos.year)
      #Retirando a pontuação
      tokenize = RegexpTokenizer(r'\w+')
      #t = tokenize.tokenize(infos.term_1.lower())
      #print(t)
    
      #tokens = tokenize.tokenize(infos.te)   
      term = infos.term_1.lower() + " " + infos.term_2.lower()
    
      #tokens.append(infos.term_1.lower() )
      #tokens.append( infos.term_2.lower() )
      print(tokens)
      tokens = tokenize.tokenize(term)  
      insert_area_dictionary_db(tokens)
        #print(list_word)    
  #print(lista_word_bigram)    
  i1 =0
  for item in list_word_area:
    #print(item)
    #print(list_word[item].get("frequencia"))
    i1 = i1+1
    if ((i1 % 100)==0):
      print("Total Term: "+str(i1) )
    sql = """
    INSERT into public.area_dictionary (term,frequency) 
    values('%s','%s');""" % (item.lower(),list_word_area[item].get("frequencia"))

    sgbdSQL.execScript_db(sql)

  i2 = 0
  for item_bigram in lista_word_area_bigram:
    i2 = i2+1
    if ((i2 % 100)==0):
      print("Total Term: "+str(i2) )
   
   
    
    frequencia =lista_word_area_bigram[item_bigram].get("frequencia")
    term_1 = lista_word_area_bigram[item_bigram].get("term_1")
    term_2 = lista_word_area_bigram[item_bigram].get("term_2") 
    sql = """
    INSERT into public.area_dictionary_bigram (term_1,term_2,frequency) 
    values('%s','%s','%s');""" % (term_1,term_2,frequencia)

    sgbdSQL.execScript_db(sql)

def insert_area_dictionary_db(tokens):

      stopwords_portuguese = nltk.corpus.stopwords.words('portuguese')
      stopwords_english = nltk.corpus.stopwords.words('english')

      word_previous = ""
      for word in tokens:
        if not((word.lower() in stopwords_portuguese) or (word.lower() in stopwords_english)):
            if(word.lower() in list_word_area):

              frequencia = (list_word_area [str(word.lower())].get("frequencia"))+1

              list_word_area [word.lower()] = {"frequencia":frequencia}
    
            else:  

              list_word_area [word.lower()] = {"frequencia":1}

            if (word_previous!=""):         
               key = word_previous + "."+ word
               
               if(key.lower() in lista_word_area_bigram):

                frequencia_bigram = (lista_word_area_bigram[key.lower()].get("frequencia"))+1
                term_1 = lista_word_area_bigram[key.lower()].get("term_1")
                term_2 = lista_word_area_bigram[key.lower()].get("term_2")
                
                lista_word_area_bigram [key.lower()] = {"term_1":term_1,"term_2":term_2,"frequencia":frequencia_bigram} 
               else:  
                lista_word_area_bigram [key.lower()] = {"term_1":word_previous.lower(),"term_2":word.lower(),"frequencia":1}

            word_previous=word
# Função para consultar o Banco SIMCC
def create_researcher_dictionary_db(test,article):
 

  #sql = "DELETE FROM research_dictionary "
  #sgbdSQL.execScript_db(sql) 

  #sql = "DELETE FROM research_dictionary_bigram "
  #sgbdSQL.execScript_db(sql) 

  filter=""
  if (test==1):
      filter =  "  and b.created_at>= '%s'" % dataP
      #+ "  OR "+researcher_teste2
        
  sql="SELECT   r.id from researcher r,bibliographic_production b where r.id=researcher_id " + filter 
  reg = sgbdSQL.consultar_db(sql)

  logger.debug(sql)
                 
  df_bd = pd.DataFrame(reg, columns=['id'])
  m=[];



  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Article: "+str(i) )
      
      create_researcher_title_dictionary_db(infos.id,article)   
     
  #print(list_word)    
  #print(lista_word_bigram)    
  i1 =0
  for item in list_word:
    #print(item)
    #print(list_word[item].get("frequencia"))
    i1 = i1+1
    if ((i1 % 100)==0):
      print("Total Term: "+str(i1) )
    sql = """
    INSERT into public.research_dictionary (term,frequency,type_) 
    values('%s','%s','%s');""" % (item.lower(),list_word[item].get("frequencia"),list_word[item].get("type"))
    
    sgbdSQL.execScript_db(sql)
    logger.debug(sql)



  list_word.clear()
  print(list_word)
  

  
 
  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Abstract: "+str(i) )

      create_researcher_abstract_dictionary_db(infos.id)    


  i1 =0

  
  

  for item in list_word:
    #print(item)
    #print(list_word[item].get("frequencia"))
    i1 = i1+1
    if ((i1 % 100)==0):
      print("Total Term: "+str(i1) )
    sql = """
    INSERT into public.research_dictionary (term,frequency,type_) 
    values('%s','%s','%s');""" % (item.lower(),list_word[item].get("frequencia"),list_word[item].get("type"))

    sgbdSQL.execScript_db(sql)
    logger.debug(sql)


  i2 = 0
  '''
  for item_bigram in lista_word_bigram:
    i2 = i2+1
    if ((i2 % 100)==0):
      print("Total Term: "+str(i2) )
   
   
    
    frequencia =lista_word_bigram[item_bigram].get("frequencia")
    term_1 = lista_word_bigram[item_bigram].get("term_1")
    term_2 = lista_word_bigram[item_bigram].get("term_2") 
    sql = """
    INSERT into public.research_dictionary_bigram (term_1,term_2,frequency) 
    values('%s','%s','%s');""" % (term_1,term_2,frequencia)

    SimccBD.execScript_db(sql)
  '''  



# Função para consultar o Banco SIMCC
def create_researcher_dictionary_abstract_db(test):
 
  
  filter=""
  if (test==1):
      filter =  " where created_at>= '%s'" % dataP
      #+ "  OR "+researcher_teste2
        
  sql="SELECT   id from researcher r" + filter
  reg = sgbdSQL.consultar_db(sql)
  logger.debug(sql)               
  df_bd = pd.DataFrame(reg, columns=['id'])
  m=[];



  
  


  list_word.clear()
  print(list_word)
    
 
  for i,infos in df_bd.iterrows():
      if ((i % 100)==0):
        print("Total Pesquisador Abstract: "+str(i) )

      create_researcher_abstract_dictionary_db(infos.id)    


  i1 =0

  
  

  for item in list_word:
    #print(item)
    #print(list_word[item].get("frequencia"))
    i1 = i1+1
    if ((i1 % 100)==0):
      print("Total Term: "+str(i1) )
    sql = """
    INSERT into public.research_dictionary (term,frequency,type_) 
    values('%s','%s','%s');""" % (item.lower(),list_word[item].get("frequencia"),list_word[item].get("type"))

    sgbdSQL.execScript_db(sql)
    logger.debug(sql)
    




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
              if(word.lower() in list_word):

                frequencia = (list_word [str(word.lower())].get("frequencia"))+1

                list_word [word.lower()] = {"frequencia":frequencia,"type":type}
      
              else:  

                list_word [word.lower()] = {"frequencia":1,"type":type}
'''
              if (word_previous!=""):         
                key = word_previous + "."+ word
                
                if(key.lower() in lista_word_bigram):

                  frequencia_bigram = (lista_word_bigram[key.lower()].get("frequencia"))+1
                  term_1 = lista_word_bigram[key.lower()].get("term_1")
                  term_2 = lista_word_bigram[key.lower()].get("term_2")
                  
                  lista_word_bigram [key.lower()] = {"term_1":term_1,"term_2":term_2,"frequencia":frequencia_bigram} 
                else:  
                  lista_word_bigram [key.lower()] = {"term_1":word_previous.lower(),"term_2":word.lower(),"frequencia":1}

              word_previous=word
'''        



Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "logfile_Population.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.DEBUG)

logger = logging.getLogger()

       
logger.debug("Inicio")
try:

  lattes10.lattes_10_researcher_frequency_db(logger)
except Exception as e: 
          print (e)         
          traceback.print_exc()   

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
#create_researcher_dictionary_db(teste,article)
#create_researcher_dictionary_abstract_db(teste)


#Levenshtein Distance

#sql = "DELETE FROM researcher_frequency "
#sgbdSQL.execScript_db(sql)            
#insert_researcher_frequency_bigrama_db(1)

"""
for i in range(4928):
  if (i%100)==0:
      print(i)
      t=threading.Thread(target=insert_researcher_frequency_db,args=(teste,article,i), name='t'+str(i))
      t.start()
      print("Thread ativa %x " % threading.active_count())
      #t.start()

      #insert_researcher_frequency_db(1)

"""


#lista = {}
#lista ['Robótica'] = {"frequencia":1}
#lista ['Educacao'] = {"frequencia":0}

#print(lista['Robótica'].get("termo"))
#print(lista['Robótica'].get("frequencia"))
#lista['Robótica'].update( {"frequencia":12})
#print(lista['Robótica'].get("frequencia"))




