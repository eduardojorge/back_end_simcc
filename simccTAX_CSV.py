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
import unidecode
import Dao.util as util


import project as project_
import sys
from Model.Year_Barema import Year_Barema

project_.project_="4"




def lista_researcher_patent_db(tax,term_p,term_i):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     #print(text)
    
     term_p = unidecode.unidecode(term_p.lower())
     term_i = unidecode.unidecode(term_i.lower())

     filter_p = util.filterSQLRank2(term_p,";","p.title")
     filter_i = util.filterSQLRank2(term_i,";","p.title")
     filter_p = filter_p[5:len(filter_p)]
     filter_i = filter_i[5:len(filter_i)]

     #filter= util.filterSQL(text,";","or","gae.name")
     




     sql="""

     SELECT DISTINCT    r.id as researcher_id ,r.institution_id,r.city_id,
               
               
                          '%s' as terms,p.title,p.development_year as year,'%s' as tax
                          FROM  researcher r  ,
                          patent p, researcher_patent_frequency rpf
                           WHERE 
                         
                           rpf.researcher_id = r.id
                           AND p.id = rpf.patent_id

                           AND ( %s or %s )

                         
                      
     
     """   % (term_p,tax,filter_p,filter_i)

     print(sql)
    

     reg = sgbdSQL.consultar_db(sql)    

     df_bd = pd.DataFrame(reg, columns=['researcher_id','institution_id','city_id',
                                        'terms','title','year','tax'])
     print (df_bd)
     return df_bd  



 
# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def list_researchers_article_abstract_tax_db(tax,term_p,term_i,type):
     



      
     term_p = unidecode.unidecode(term_p.lower())
     term_i = unidecode.unidecode(term_i.lower())

     filter_p = util.filterSQLRank2(term_p,";","title")
     filter_i = util.filterSQLRank2(term_i,";","title")
     filter_p = filter_p[5:len(filter_p)]
     filter_i = filter_i[5:len(filter_i)]
    

     
     
     
   
    
           
        
     
     #filter= util.filterSQLRank(terms,";","or","rf.term","title")


     
     df_bd=pd.DataFrame()
     if (type=='ARTICLE'):
                          #filter= util.filterSQLRank(terms,";",boolean_condition,"rf.term","title")
                          # researcher_frequency rf,
                           # AND rf.researcher_id = r.id 
                        
                          #  AND b.id = rf.bibliographic_production_id
                            
                          sql = """SELECT DISTINCT r.id as id,b.title,
                           r.institution_id,
                           r.city_id,
                          '%s' as terms,b.year,ba.qualis,'%s' as tax
                        
                           FROM  researcher r ,
                          
                           bibliographic_production b,
                           bibliographic_production_article ba
                           WHERE 
                            b.id= ba.bibliographic_production_id
                            AND r.id = b.researcher_id
                          
                          
                           AND (%s or %s)
                          
     
                         
                          

                          
                           ORDER BY year desc""" % (term_p,tax,filter_p,filter_i)
                          print(sql)
                          reg = sgbdSQL.consultar_db(sql)
                          df_bd = pd.DataFrame(reg, columns=['researcher_id','title',
                                                             'institution_id','city_id',
                                                             'terms','year','qualis','tax'])
                           
                         

                          
     
     if (type=='ABSTRACT'):
                           filter_p = util.filterSQLRank2(term_p,";","abstract")
                           filter_i = util.filterSQLRank2(term_i,";","abstract")
                           filter_p = filter_p[5:len(filter_p)]
                           filter_i = filter_i[5:len(filter_i)]
                           # researcher_abstract_frequency rf,
                           # filter= util.filterSQLRank2(terms,";","or","rf.term","abstract")
                            #AND (translate(unaccent(LOWER(rf.term)),\':\',\'\') ::tsvector@@ \''%s'\'::tsquery)=true
                           #  rf.researcher_id = r.id
                           sql ="""SELECT distinct r.id,r.abstract,   r.institution_id,
                           r.city_id, '%s' as terms,'%s' as tax
                        
                           FROM   researcher r ,
                            institution i
                          where
                         
                       
                          AND ( %s or %s)
                       
                                                
                          """ % (term_p,tax,filter_p,filter_i)
                           print(sql)
                           reg = sgbdSQL.consultar_db(sql)
                           df_bd = pd.DataFrame(reg, columns=['researcher_id','abstract',
                                                             'institution_id','city_id',
                                                             'terms','tax'])

     
     return df_bd



      

#df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
df = pd.read_excel(r'files/tEnergiasRenovaveis.xlsx')
print(df)
TAX=0
TERMOS_P=1
TERMOS_I=2
x=0

for i,infos in df.iterrows():
   

    print("teste x "+ str(infos[TAX]))
    
    #df1= list_researchers_article_abstract_tax_db(str(infos[TERMOS]),"ABSTRACT")
    df1_patent =  lista_researcher_patent_db(str(infos[TAX]),str(infos[TERMOS_P]),str(infos[TERMOS_I]))
    df1_article =  list_researchers_article_abstract_tax_db(str(infos[TAX]),str(infos[TERMOS_P]),str(infos[TERMOS_I]),"ARTICLE")
    df1_abstract =  list_researchers_article_abstract_tax_db(str(infos[TAX]),str(infos[TERMOS_P]),str(infos[TERMOS_I]),"ABSTRACT")



    if(x!=0):
          df_patent = pd.concat([df_patent, df1_patent], axis=0, join='inner')
          df_article = pd.concat([df_article, df1_article], axis=0, join='inner')
          df_abstract = pd.concat([df_abstract, df1_abstract], axis=0, join='inner')
    else:
         df_patent=df1_patent   
         df_article=df1_article   
         df_abstract=df1_abstract    

 
     
    x=x+1
#print(df)    
print("Fim "+str(x) )   
df_patent.to_csv('c:\\simccv3\\patent_tax.csv')      
df_article.to_csv('c:\\simccv3\\article_tax.csv')      
df_abstract.to_csv('c:\\simccv3\\abstract_tax.csv')  





#print(termFlowSQL.list_researchers_originals_words_db("biomassa","","ABSTRACT","or",""))
#print(areaFlowSQL.lista_researcher_patent_db("biomassa","",""))


