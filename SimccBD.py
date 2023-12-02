#import requests
import json
import pandas as pd
import psycopg2
import nltk
from nltk.tokenize import RegexpTokenizer
import unidecode
import Dao.sgbdSQL as sgbdSQL
import Dao.util as util

researcher_teste1 ="r.name LIKE \'Eduardo Manu%\'"
researcher_teste2 ="r.name LIKE \'Hugo Saba%\'"



# Função para consultar o Banco SIMCC
def researcher_text_db():
  reg = sgbdSQL.consultar_db('SELECT   id from researcher '+
                    'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    ' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
  df_bd = pd.DataFrame(reg, columns=['id'])
  m=[];
  for i,infos in df_bd.iterrows():
      print(infos.id)
      m.append(production_db(infos.id))
  return m     

# Função para consultar o Banco SIMCC
def term_frequency_substring_db():
  reg = sgbdSQL.consultar_db('SELECT   id from researcher '+
                    'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    ' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
  df_bd = pd.DataFrame(reg, columns=['id'])
  m=[];
  for i,infos in df_bd.iterrows():
      print(infos.id)
      m.append(production_db(infos.id))
  return m     





# Função para consultar o Banco SIMCC
def production_db(researcher_id):
  reg = sgbdSQL.consultar_db('SELECT  distinct title,b.type,year from bibliographic_production AS b, bibliographic_production_author AS ba'+
  ' WHERE b.id = ba.bibliographic_production_id '+
    '  AND researcher_id=\''+ researcher_id +"\'"+
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
      print(infos.title) 
      # Teste de Algoritmo tokens = tokenize.tokenize("Eu sou a pessoa mais feliz pessoa sou.")    
      #retirando termos duplicados
      lista=[]
      lista = list(set(tokens))
      print(lista)
      for word in lista: 
          texto = texto + " " + word;
          #print("-----")
          #print(word)
 


  linha=[]
  linha.append(researcher_id)   
  linha.append(texto)
  #print(researcher_id)
  #print(texto)
   
  return  linha
 


# Função para consultar o Banco SIMCC
def bibliographic_production_total_db():
  reg = sgbdSQL.consultar_db('SELECT COUNT(DISTINCT title) as qtd FROM bibliographic_production ')
                    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
  df_bd = pd.DataFrame(reg, columns=['qtd'])
 
  return df_bd['qtd'].iloc[0]   

# Função para consultar o Banco SIMCC
def researcher_total_db():
  reg = sgbdSQL.consultar_db('SELECT COUNT(*) as qtd FROM researcher ')
                    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
  df_bd = pd.DataFrame(reg, columns=['qtd'])
 
  return df_bd['qtd'].iloc[0]   

# Função para consultar o Banco SIMCC
def institution_total_db():
  reg = sgbdSQL.consultar_db('SELECT COUNT(*) as qtd FROM institution ')
                    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
                    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
  df_bd = pd.DataFrame(reg, columns=['qtd'])
 
  return df_bd['qtd'].iloc[0]   

# Função para listar a palavras  passando as iniciais de um radica
def list_originals_words_initials_term_db(initials):
     initials=unidecode.unidecode(initials)
   
     reg = sgbdSQL.consultar_db('SELECT  originals_words '+
                      
                         ' FROM researcher_term rt '+
                          ' WHERE '+
                         '  LOWER(unaccent(term)) LIKE \''+initials+'%\'')
                         
     df_bd = pd.DataFrame(reg, columns=['originals_words'])
     text=""
     for i,infos in df_bd.iterrows():
         text=text+infos.originals_words
     #text = str(len(df_bd.index)) + text
     tokenize = RegexpTokenizer(r'\w+')
     tokens =[]
     #tokens = tokenize.tokenize( str(len(df_bd.index)))  
     tokens = tokenize.tokenize(text)  

     lista=[]
     
    
     lista = list(set(tokens))
    
     print(lista)


    
     return lista



# Função para listar a palavras  passando as iniciais de um radica
def list_sub_area_expertise_initials_term_db(initials):
     initials=unidecode.unidecode(initials)
   
     reg = sgbdSQL.consultar_db('SELECT  name as word '+
                      
                         ' FROM sub_area_expertise sub '+
                          ' WHERE '+
                         '  LOWER(unaccent(name)) LIKE \''+initials.lower()+'%\''+
                         ' AND char_length(unaccent(LOWER(name)))>3 AND to_tsvector(\'portuguese\', unaccent(LOWER(name)))!=\'\' and  unaccent(LOWER(name))!=\'sobre\' ')
                         
     df_bd = pd.DataFrame(reg, columns=['word'])
    
    
    

    
    
     return  df_bd

# Função para listar a palavras  passando as iniciais de um radica
def list_researcher_sub_area_expertise_db(sub_area_experise):
     initials=unidecode.unidecode(sub_area_experise)
   
     reg = sgbdSQL.consultar_db('SELECT  sub.name as word '+
                      
                         ' FROM sub_area_expertise sub,researcher_area_expertise rae '+
                          ' WHERE '+
                         '  LOWER(unaccent(sub.name))=\''+sub_area_experise.lower()+'\''+
                         ' AND sub.id = rae.sub_area_expertise_id')
                         
     df_bd = pd.DataFrame(reg, columns=['researcher_id'])
    
    
    

    
    
     return  df_bd

 # Função para listar as areas de conhecimento por uma parte da palavra
def list_area_expertise_term_db(term):
     
     term=unidecode.unidecode(term)
     '''
   
     reg = consultar_db("SELECT (a.name || \' | \'|| b.name)  AS area_expertise_, b.id as id  from area_expertise a,  area_expertise b "+

                       " WHERE a.id = b.parent_id "+
                       " AND a.parent_id IS NOT NULL "+

                       " AND unaccent(LOWER(a.name)) LIKE \'"+term.lower()+"%' "+

                      " ORDER BY a.name,b.name ")

                      '''
     
     reg = sgbdSQL.consultar_db("SELECT (a.name)  AS area_expertise_, a.id as id  from area_expertise a "+

                       " WHERE "+
                       

                       "  unaccent(LOWER(a.name)) LIKE \'"+term.lower()+"%' "+

                      " ORDER BY a.name ")

                      
                        
     df_bd = pd.DataFrame(reg, columns=['area_expertise_','id'])

     return df_bd

# Função para listar os pesquisadores por suas areas de conhecimento
def list_researcher_area_expertise_term_db(term):
     term=unidecode.unidecode(term)
   
     reg = sgbdSQL.consultar_db("SELECT unaccent((a.name || ' | ' || b.name))  AS area_expertise , r.name "+
                        " from area_expertise a,  area_expertise b, researcher r, researcher_area_expertise rae "+
                        " WHERE a.id = b.parent_id " +
                        " AND r.id = rae.researcher_id "+
                        " AND rae.area_expertise_id = b.id "+
                        " AND a.parent_id IS NOT NULL "+
 
                        " AND (       translate(unaccent(LOWER((a.name ||' ' || b.name))),':','') ::tsvector@@ '"+term+"'::tsquery)=true")

                      
                        
     df_bd = pd.DataFrame(reg, columns=['area_expertise','r.name'])

     return df_bd





 



 
 


# Função para Pesquisar os pesquisadores por nome
# https://www.postgresql.org/docs/9.3/textsearch-intro.html#TEXTSEARCH-DOCUMENT
def lista_researcher_name_db(text):
     tokenize = RegexpTokenizer(r'\w+')
     tokens =[]
     tokens = tokenize.tokenize(text)  
     term=""
     print(tokens.count)
     #if tokens.count>1:
     for word in tokens:
            term = term + word + " & "
     #else:
        #term = tokens[0]   
     x = len(term)   
     termX = term[0:x-3]
     print(termX)  
    

   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     
     reg = sgbdSQL.consultar_db('SELECT rf.researcher_id as id,COUNT(rf.term) AS qtd,'+
                        'r.name as researcher_name,i.name as institution,rp.articles as articles,rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes'+
                         ' FROM researcher_frequency rf, researcher r , institution i, researcher_production rp '+
                          ' WHERE '+
                          ' rf.researcher_id = r.id'
                          ' AND r.institution_id = i.id '+
                          ' AND rp.researcher_id = r.id '+
                          ' AND (translate(unaccent(LOWER(r.name)),\':\',\'\') ::tsvector@@ \''+unidecode.unidecode(text)+'\'::tsquery)=true '+
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          ' GROUP BY rf.researcher_id,r.name, i.name,articles, book_chapters,book,r.lattes_id'+
                          ' ORDER BY qtd desc')






     df_bd = pd.DataFrame(reg, columns=['id','qtd','researcher_name','institution','articles','book_chapters','book','lattes'])
     return df_bd



                          
    

def lista_researcher_full_name_db_(text,graduate_program_id):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

 
     filter = ""
     """
     if text!="": 
  
      t=[]
      t= text.split(";")  
     
      i=0;
      for word in t:
          #filter = "LOWER(r.name)='"+word.lower()+"' or "+ filter
          filter = "translate(LOWER(r.name),'''','')='"+word.lower().replace("'","")+"' or "+ filter
          i=i+1
      x = len(filter)   
      filter = filter[0:x-3]
      filter = "AND ("+filter+")" 
      """
     
     t= text.split(";")  
     filter = ""
     i=0;

     if (len(t))==1:
           filter =  " and LOWER(r.name) like '"+t[0]+"%'"
         
     else:    
        
          filter= util.filterSQLRank(text,";","or","r.name","r.name")     

     print(filter)  
   

     sql="""SELECT distinct r.id as id,
             r.name as researcher_name,i.name as institution,rp.articles as articles,
             rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,
             r.abstract as abstract,rp.great_area as area,rp.city as city, i.image as image,r.orcid as orcid, 
             r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
             TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update
             FROM  researcher  r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id  , city c, 
                          
             institution i, researcher_production rp 
                           
                           WHERE
                          
                            c.id=r.city_id
                     

                           AND r.institution_id = i.id 
                           AND rp.researcher_id = r.id %s %s """ % (filter,filtergraduate_program)
     print(sql)
     reg = sgbdSQL.consultar_db(sql)
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          #' GROUP BY rf.researcher_id,r.name, i.name,articles, book_chapters,book,r.lattes_id,r.lattes_10_id,r.abstract,gae.name'+
                          #' ORDER BY qtd desc')



     df_bd = pd.DataFrame(reg, columns=['id','researcher_name','institution','articles',
                                        'book_chapters','book','lattes','lattes_10_id','abstract','area','city','image','orcid',
                                        'graduation','patent','software','brand','lattes_update'])
   
     return df_bd



# Função que lista  os nomes dos pesquisadores por iniciais


def lists_researcher_initials_term_db(initials,graduate_program_id):
     
     initials=unidecode.unidecode(initials)
     """
     t= initials.split(";")
     filter=""
     for word in t:
         filter= "(        translate(unaccent(LOWER(r.name)),\':;\',\'\') ::tsvector@@ unaccent(LOWER('"+word.lower()+"'))::tsquery)=TRUE  and"+ filter
       
     x = len(filter)   
     filter = filter[0:x-3]
     filter = "("+filter+")" 
     """
     filter = util.filterSQLRank2(initials,";","or","r.name","r.name")


     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

 
     sql="""SELECT distinct id, name as nome FROM PUBLIC.researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id 
     WHERE   r.institution_id is NOT null %s %s 
     order by nome """ % (filter,filtergraduate_program)
     #(filter,initials.lower()+"%",filtergraduate_program)
     #( %s OR unaccent(LOWER(r.name)) LIKE '%s' )
     print(sql)
                        
                        
                        ## LOWER(unaccent(name)) LIKE \'%"+initials+"%\' order by nome")
     reg = sgbdSQL.consultar_db(sql)                     
     df_bd = pd.DataFrame(reg, columns=['id','nome'])
     
     #print(df_bd)
     return df_bd




# Função que lista  as produções de artigos por Tipo e Ano


def lists_bibliographic_production_article_db(term,year,qualis,institution,distinct,graduate_program_id):
     


    filterinstitution=util.filterSQL(institution,";","or","i.name")
  
    term=unidecode.unidecode(term.lower())

    filter = util.filterSQLRank(term,";","or","rf.term","title")

    filterQualis = util.filterSQL(qualis,";","or","qualis")

    filtergraduate_program=""
    if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id
    #distinct=""
  
    
    if distinct=="1":
       

        sql= """ SELECT distinct title,year_,doi,qualis,periodical_magazine_name as magazine,jcr, jcr_link 
        FROM institution i,researcher_frequency rf, PUBLIC.bibliographic_production b, bibliographic_production_article a,
        researcher r  LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id
        WHERE  r.id=b.researcher_id 
        
        AND  b.id = rf.bibliographic_production_id 
        AND a.bibliographic_production_id = b.id 
        AND i.id = r.institution_id %s %s %s %s
        AND year_ >=%s
        AND  b.type = \'ARTICLE\' order by year_ desc""" % (filter,filterinstitution,filtergraduate_program,filterQualis,year)
        print(sql)
        
        reg = sgbdSQL.consultar_db(sql)

        df_bd = pd.DataFrame(reg, columns=['title','year','doi','qualis','magazine','jcr', 'jcr_link'])
        print(df_bd)
        return df_bd
    
    if distinct=="0":
   
       sql =""" SELECT distinct title,year_,doi,qualis,periodical_magazine_name as magazine,r.name as researcher,
       r.lattes_10_id as lattes_10_id,r.lattes_id as lattes_id,jcr, jcr_link 
       FROM institution i,researcher_frequency rf, PUBLIC.bibliographic_production b, bibliographic_production_article a,
       researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id
       WHERE  r.id=b.researcher_id 
      
       AND  b.id = rf.bibliographic_production_id 
       AND a.bibliographic_production_id = b.id 
       AND i.id = r.institution_id %s %s %s %s 
       AND year_ >=%s
       AND  b.type = \'ARTICLE\' order by year_ desc""" %  (filter,filterinstitution,filtergraduate_program,filterQualis,year)
       print(sql)
       reg = sgbdSQL.consultar_db(sql) 
                     
       df_bd = pd.DataFrame(reg, columns=['title','year','doi','qualis','magazine','researcher','lattes_10_id','lattes_id','jcr', 'jcr_link'])     
       #print(df_bd)
       return df_bd



    
                       
                        
                        
                     
    
    


def lists_bibliographic_production_article_name_researcher_db(name,year,qualis):
     
     name=unidecode.unidecode(name.lower())
      
     t=[]
     t= name.split(";")  
     filter = ""
     i=0;
     for word in t:
         filter = "unaccent(LOWER(r.name))='"+word.lower()+"' or "+ filter
         i=i+1
     x = len(filter)   
     filter = filter[0:x-3]
     filter = "("+filter+")" 






     if qualis=="":
         filterQualis=""
     else:  
         
          t=[]
          t= qualis.split(";")  
          filterQualis = ""
          i=0;
          for word in t:
              filterQualis = "unaccent(LOWER(qualis))='"+word.lower()+"' or "+ filterQualis
              i=i+1
          x = len(filterQualis)   
          filterQualis = filterQualis[0:x-3]
          filterQualis = " AND ("+filterQualis+")" 

        

     reg = sgbdSQL.consultar_db(  " SELECT distinct bp.id as id,title,year,doi,qualis,r.name as researcher, m.name as magazine"+
                         " FROM researcher r, PUBLIC.bibliographic_production bp, bibliographic_production_article a,periodical_magazine m "+
                          "  WHERE m.id = a.periodical_magazine_id "+
                     
                        " AND bp.researcher_id = r.id "+
                        "   AND a.bibliographic_production_id = bp.id "+
                        " AND"+ filter+
                         "  AND year_ >=%s" % year+
                         filterQualis+
                        
                        
     
                        "  AND  bp.type = \'ARTICLE\' ")
                       
                        
                        
                     
     df_bd = pd.DataFrame(reg, columns=['id','title','year','doi','qualis','researcher','magazine'])
     print(df_bd)
     return df_bd


     
def lista_researcher_full_name_db():
    
   
    
  

     reg = sgbdSQL.consultar_db('SELECT distinct r.id as id,'+
                       
                        ' r.lattes_id as lattes,r.lattes_10_id as lattes_10_id'+
                        
                         
                         ' FROM  researcher r  '
                       
                      
                        )
                          



     df_bd = pd.DataFrame(reg, columns=['id','lattes','lattes_10_id'])
     df_shuffled =df_bd.sample(frac=1)
     
   
     return df_shuffled














#lists_word_researcher_db("")

#lists_bibliographic_production_article_db("robotica;educacao",2018,"A1;A2")
#lista_articles_area_expertise_db("ciencias exatas e da terra","robótica",1900,"")

#lista_researcher_area_expertise_db("ciencias exatas e da terra","Universidade do Estado da Bahia;Universidade Estadual de Feira de Santana")

#lista_researcher_area_expertise_db("ciencias exatas e da terra","")
#lista_institution_production_db("Robótica;Educacional")        
#lists_bibliographic_production_article_researcher_db("Robótica",'35e6c140-7fbb-4298-b301-c5348725c467')
#lists_bibliographic_production_article_researcher_db("",'35e6c140-7fbb-4298-b301-c5348725c467')
#lists_bibliographic_production_qtd_qualis_researcher_db('35e6c140-7fbb-4298-b301-c5348725c467',2020)
         
    
###lists_bibliographic_production_article_name_researcher_db("Eduardo Manuel de Freitas Jorge;Eduardo Alfredo Morais Guimaraes",2020,"")

###print(list_sub_area_expertise_initials_term_db("Arq"))
#print( list_researcher_sub_area_expertise_db("Arquitetura De Computadores"))
    
#lists_bibliographic_production_qtd_qualis_researcher_db("",2020)







    
     




                  




          #print("-----")
          #print(word)

  #print(researcher_id)
  #print(texto)
   
  
#print(list_originals_words_initials_term_db("robô")) 
#print(list_researchers_originals_words_db("robótica | robô | robotics"))   
#print(list_area_expertise_term_db("banco"))
#print(list_researcher_area_expertise_term_db("banco & dados"))
#print(list_researchers_originals_words_db("pibid","Universidade do Estado da Bahia;Universidade Estadual de Feira de Santana"))
#lista_institution_area_expertise_db("ciencias exatas e da terra","Universidade do Estado da Bahia;Universidade Estadual de Feira de Santana")
#print(lista_production_article_area_expertise_db("ciencias exatas e da terra","2020","A1;A2;A3"))
##print(lista_production_article_area_expertise_db("ciencias sociais aplicadas","2021","A1"))

###lista_production_article_area_expertise_db
#lista_institution_area_expertise_db("ciencias exatas e da terra,"")
#print(list_researchers_originals_words_db("Robótica",""))
#create_researcher_dictionary_db()
#create_area_ditionary_db()
#create_researcher_production_db()
#print(list_research_dictionary_db("rob"))

###print(lista_researcher_full_name_db("Eduardo Manuel de Freitas Jorge"))

#print(lista_researcher_area_expertise_db("Ciencias Exatas e da Terra",""))

###print(list_researchers_originals_words_db("Robotica",""))


#print(list_area_expertise_term_db("Edu"));

#print(list_researchers_originals_words_db("computer",""))
