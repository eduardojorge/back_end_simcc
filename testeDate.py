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
           


def lista_researcher_patent_db(text,institution,graduate_program_id):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     print(text)
     text = text.replace("&"," ")
     text = unidecode.unidecode(text.lower())

     filter = util.filterSQLRank2(text,";","p.title")
     #filter= util.filterSQL(text,";","or","gae.name")
     

     filterinstitution= util.filterSQL(institution,";","or","i.name")
     print("XXXXXXXXXXXXXXXXXXXXX" +text)
     print(filterinstitution)


     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id


     sql="""

     SELECT DISTINCT rp.great_area as area,rp.area_specialty as area_specialty, r.id as id,
               r.name as researcher_name,i.name as institution,rp.articles as articles,
                         rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,
                        r.orcid as orcid,rp.city  as city, i.image as image,'%s' as terms,p.title,p.development_year as year
                          FROM  researcher r  LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id 
                         , institution i, researcher_production rp,patent p, researcher_patent_frequency rpf, city c 
                           WHERE 
                         
                       
                           r.city_id=c.id
                 
                           AND r.institution_id = i.id 
                           AND rp.researcher_id = r.id 
                           AND p.researcher_id = r.id
                           AND rpf.researcher_id = r.id

                           %s %s %s

                         
                      
     
     """   % (text,filter,filterinstitution,filtergraduate_program)

     print(sql)
    

     reg = sgbdSQL.consultar_db(sql)    

     df_bd = pd.DataFrame(reg, columns=['area','area_specialty','id',
                                        'researcher_name','institution','articles','book_chapters','book',
                                        'lattes','lattes_10_id','abstract','orcid','city','image','terms','title','year'])
     print (df_bd)
     return df_bd  



 
# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def list_researchers_article_tax_db(terms,type):
     print(terms)

     
     
     

    
           
        
     
     filter= util.filterSQLRank(terms,";","title")


     
     df_bd=pd.DataFrame()
     #researcher_frequency rf,
     # AND rf.researcher_id = r.id 
     if (type=='ARTICLE'):
                          #filter= util.filterSQLRank(terms,";",boolean_condition,"rf.term","title")
                            
                          sql = """SELECT DISTINCT rf.researcher_id as id,b.title,
                           r.institution_id,
                           r.city_id,
                          '%s' as terms,b.year,ba.qualis
                        
                           FROM  researcher r ,
                           
                           bibliographic_production b,
                           bibliographic_production_article ba
                           WHERE 
                            b.id= ba.bibliographic_production_id
                          
                          
                           %s 
                          
     
                          
                        
                            AND b.id = rf.bibliographic_production_id
                          

                          
                           ORDER BY year desc""" % (terms,filter)
                          print(sql)
                          reg = sgbdSQL.consultar_db(sql)
                          df_bd = pd.DataFrame(reg, columns=['researcher_id','title',
                                                             'institution_id','city_id',
                                                             'terms','year','qualis'])
                           
                         

                          
     
     if (type=='ABSTRACT'):
                          filter= util.filterSQLRank2(terms,";","abstract")
                          #  researcher_abstract_frequency rf,
                          #   AND rf.researcher_id = r.id
                            #AND (translate(unaccent(LOWER(rf.term)),\':\',\'\') ::tsvector@@ \''%s'\'::tsquery)=true
                          sql ="""SELECT distinct r.id as id,0 as qtd,
                          r.name as researcher_name,i.name as institution,rp.articles as articles,rp.book_chapters as book_chapters, rp.book as book,
                          r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,abstract,rp.great_area as area,rp.city as city,r.orcid as orcid,i.image as image,
                          r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
                           TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update, '%s' as terms
                        
                         FROM   researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id , 
                         institution i, researcher_production rp, city c
                         WHERE 
                         c.id = r.city_id
                         %s 
                      
                         AND r.institution_id = i.id 
                         AND rp.researcher_id = r.id 
                         
                       
                          ORDER BY qtd desc """ % (terms,filter)
                          print(sql)
                          reg = sgbdSQL.consultar_db(sql)
                          df_bd = pd.DataFrame(reg, columns=['id','qtd','researcher_name','institution',
                                        'articles','book_chapters','book','lattes',
                                        'lattes_10_id','abstract','area','city','orcid',
                                        'image','graduation','patent','software','brand','lattes_update','terms'])

     
     return df_bd




def lists_bibliographic_production_article_researcher_db(term,researcher_id,year,type,boolean_condition,qualis):
     
     term=unidecode.unidecode(term.lower())
     filter = util.filterSQLRank(term,";","title")
     filterQualis = util.filterSQL(qualis,";","or","qualis")
     '''
     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

     '''   

      #researcher r   LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id

    
     if (type=='ARTICLE'):
           #LEFT JOIN  researcher_frequency rf ON b.id = rf.bibliographic_production_id,
           sql= """ SELECT distinct b.id as id,title,b.year as year,type, doi,qualis, periodical_magazine_name as magazine,
               r.name as researcher,r.lattes_10_id as lattes_10_id,r.lattes_id as lattes_id,jcr as jif,jcr_link 
               FROM bibliographic_production b ,
                bibliographic_production_article ba,institution i, researcher r 
                WHERE 
                r.id = b.researcher_id
               
                AND   b.id = ba.bibliographic_production_id 
                 AND r.institution_id=i.id
                AND year_>=%s  %s %s
                AND r.id=\'%s\' order by year desc""" % (year,filter,filterQualis,researcher_id)
           
           reg = sgbdSQL.consultar_db(sql)
           print(sql)
           df_bd = pd.DataFrame(reg, columns=['id','title','year','type','doi','qualis','magazine','researcher','lattes_10_id','lattes_id','jif','jcr_link'])
          
     if (type=='ABSTRACT'):
          #researcher_abstract_frequency rf, 
          # AND rf.researcher_id=r.id 

          sql="""
                SELECT distinct b.id as id,title,year,type, doi,qualis, periodical_magazine_name as magazine,r.name as researcher,
                r.lattes_10_id as lattes_10_id,r.lattes_id as lattes_id,jcr as jif,jcr_link
                          FROM bibliographic_production b, 
                          bibliographic_production_article ba, researcher r 
                            WHERE  r.id = b.researcher_id
                                  
                                   AND pm.id = ba.periodical_magazine_id 
                                   AND   b.id = ba.bibliographic_production_id 
                                   AND year_ >=%s %s %s
                                   AND r.id='%s'
                                     order by year desc"

          """ % (year,filter,filterQualis,researcher_id)
          reg = sgbdSQL.consultar_db(sql)
          df_bd = pd.DataFrame(reg, columns=['id','title','year','type','doi','qualis','magazine','researcher','lattes_10_id','lattes_id','jif','jcr_link'])
         
                        
                        
     
                       # "  AND  b.type = \'ARTICLE\' ")
                       
                        
     
                     
     
   
     return df_bd
          

#hoje = datetime.today() - timedelta(days=5)
#print(hoje.date())

#dataLattes(180)

#testeDiciopnario()
#researcher_csv_db()

#"1966167015825708;8933624812566216"

#testeSegundaPalavra("educacao")


#df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
df = pd.read_excel(r'files/tEnergiasRenovaveis.xlsx')
print(df)
TERMOS=0

x=0

for i,infos in df.iterrows():
   

    print("teste x "+ str(infos[TERMOS]))
    
    df1= list_researchers_article_tax_db(str(infos[TERMOS]),"ARTICLE")
    #df1 =  lista_researcher_patent_db(str(infos[TERMOS]),"","")


    if(x!=0):
          df = pd.concat([df, df1], axis=0, join='inner')
    else:
         df=df1       

 
     
    x=x+1
print(df)    
print("Fim "+str(x) )   
df.to_csv('c:\\simccv3\\article_tax.csv')      





#print(termFlowSQL.list_researchers_originals_words_db("biomassa","","ABSTRACT","or",""))
#print(areaFlowSQL.lista_researcher_patent_db("biomassa","",""))

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

