from datetime import datetime, timedelta
import  Dao.resarcher_baremaSQL as  resarcher_baremaSQL

import Dao.sgbdSQL as sgbdSQL
import Dao.termFlowSQL as termFlowSQL
import Dao.areaFlowSQL as areaFlowSQL
import Dao.graduate_programSQL as graduate_programSQL
import pandas as pd
import logging
import json
from datetime import datetime
import nltk
from nltk.tokenize import RegexpTokenizer
import sys


import project as project_
import sys
from Model.Year_Barema import Year_Barema

project_.project_="4"

def researcher_csv_db():

   sql=""" SELECT r.lattes_id
        

        FROM  researcher r """

   reg = sgbdSQL.consultar_db(sql)
   
   #logger.debug(sql)
        
   
   df_bd = pd.DataFrame(reg, columns=['lattes_id'])

   df_bd.to_csv('researcher_simmc.csv')


def testeDiciopnario():
      
      sql="""

            SELECT   translate(unaccent(LOWER(b.title)),':;''','') ::tsvector as palavras from researcher r,bibliographic_production b 
            WHERE b.researcher_id='9d22c219-d3f7-46a3-b4c3-ef04eb9c31c8' and b.type='ARTICLE'
         """ 
      reg = sgbdSQL.consultar_db(sql)
   
   #logger.debug(sql)
        
   
      df_bd = pd.DataFrame(reg, columns=['palavras']) 

      for i,infos in df_bd.iterrows():
           print(infos.palavras)
           
def dataLattes(dias):
      
      dataP = datetime.today() - timedelta(days=dias)
      
      sql="""

            SELECT  name from researcher where last_update <='%s'
         """ % (dataP)
      reg = sgbdSQL.consultar_db(sql)
   
   #logger.debug(sql)
        
   
      df_bd = pd.DataFrame(reg, columns=['name_']) 

      for i,infos in df_bd.iterrows():
           print(infos.name_)
           
def testeSegundaPalavra(term):
      
      sql="""

           SELECT   
                  unnest(regexp_matches (unaccent(LOWER(bp.title)), ' %s\s+(\w+)','g')) as palavras FROM bibliographic_production AS bp where bp.type='ARTICLE'
         """ % (term)
      reg = sgbdSQL.consultar_db(sql)
   
   #logger.debug(sql)
        
   
      df_bd = pd.DataFrame(reg, columns=['palavras']) 
      stopwords_portuguese = nltk.corpus.stopwords.words('portuguese')
      stopwords_english = nltk.corpus.stopwords.words('english')
      tokens =[]
      text=""
      for i,infos in df_bd.iterrows():
         
          
           

     
     
      
          if not((infos.palavras.lower() in stopwords_portuguese) or (infos.palavras.lower() in stopwords_english)):
              text = text + " " + infos.palavras
             
              print(infos.palavras)

      tokenize = RegexpTokenizer(r'\w+')    
      tokens = tokenize.tokenize(text)    
      print(tokens)
      freq =nltk.FreqDist(tokens)
      print(freq.most_common(10))
      for word in freq.most_common(10):
           print(word[0])
           print(word[1])


           sql= """SELECT b.title
            FROM bibliographic_production AS b 
           WHERE 
           b.type='ARTICLE'
        
 
           AND  ts_rank(to_tsvector(unaccent(LOWER(b.title))), websearch_to_tsquery( '%s<->%s')) > 0.099""" % (term,word[0])

           reg = sgbdSQL.consultar_db(sql)
           df_bd = pd.DataFrame(reg, columns=['title']) 
           for i,infos in df_bd.iterrows():
                print(infos.title)
           

      
          

#hoje = datetime.today() - timedelta(days=5)
#print(hoje.date())

#dataLattes(180)

#testeDiciopnario()
#researcher_csv_db()

#"1966167015825708;8933624812566216"

#testeSegundaPalavra("educacao")

#print(termFlowSQL.list_researchers_originals_words_db("energia;solar","","ARTICLE","or",""))
#print(termFlowSQL.list_researchers_originals_words_db("energia;","","ABSTRACT","or",""))
print(areaFlowSQL.lista_researcher_patent_db("DECODIFICAÇÃO;IMAGENS","",""))

"""
year = Year_Barema()
year.article="2018"
year.work_event="2018"
year.book="1900" 

year.chapter_book="1900"

year.patent="1900"
year.software="1900"
year.brand="1900"
year.resource_progress="1900"
year.resource_completed="1900"
year.participation_events="1900"



print(resarcher_baremaSQL.researcher_production_db("todos","",year))

"""

