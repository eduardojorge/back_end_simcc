from datetime import datetime, timedelta
import  Dao.resarcher_baremaSQL as  resarcher_baremaSQL

import Dao.sgbdSQL as sgbdSQL
import Dao.graduate_programSQL as graduate_programSQL
import pandas as pd
import logging
import json
from datetime import datetime
import sys


import project as project_
import sys
from Model.Year_Barema import Year_Barema

project_.project_="7"

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
           


#hoje = datetime.today() - timedelta(days=5)
#print(hoje.date())

#dataLattes(180)

#testeDiciopnario()
#researcher_csv_db()

#"1966167015825708;8933624812566216"


year = Year_Barema()
year.artigo="2018"
year.work_event="2018"
year.book="1900" 

year.chapter_book="1900"
year.book="1900"
year.patent="1900"
year.software="1900"
year.brand="1900"
year.resource_progress="1900"
year.resource_completed="1900"
year.participation_events="1900"



print(resarcher_baremaSQL.researcher_production_db("todos","",year))



