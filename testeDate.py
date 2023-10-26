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
project_.project_=sys.argv[1]

def researcher_csv_db():

   sql=""" SELECT r.lattes_id
        

        FROM  researcher r """

   reg = sgbdSQL.consultar_db(sql)
   
   #logger.debug(sql)
        
   
   df_bd = pd.DataFrame(reg, columns=['lattes_id'])

   df_bd.to_csv('researcher_simmc.csv')


hoje = datetime.today() - timedelta(days=5)
print(hoje.date())
researcher_csv_db()

#"1966167015825708;8933624812566216"

#print(resarcher_baremaSQL.researcher_production_db("Hugo Saba Pereira Cardoso;Gesil Sampaio Amarante Segundo","","year_5=2020;year_37=2019"))



