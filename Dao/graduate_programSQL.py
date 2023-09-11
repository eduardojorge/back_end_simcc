
import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util
 # Função para listar a palavras do dicionário passando as iniciais 

def graduate_program_csv_db(institution_id):

   reg = sgbdSQL.consultar_db( " SELECT graduate_program_id,code,name as program,area,modality,type,rating "
       " FROM graduate_program gp where institution_id=\'%s\'" % institution_id)
        
   
   df_bd = pd.DataFrame(reg, columns=[ 'graduate_program_id','code','program','area','modality','type','rating'])

   print(df_bd)

   return df_bd





