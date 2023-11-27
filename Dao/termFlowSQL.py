
import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util


# Função que lista  as áreas de expertrize por Iniciais

def get_researcher_address_db(researcher_id):
     
     reg = sgbdSQL.consultar_db("SELECT distinct city,organ from researcher_address ra "
                                 "  WHERE "
                                 
                                 "  ra.researcher_id='%s'" % researcher_id)
                         
     df_bd = pd.DataFrame(reg, columns=['city','organ'])
   
     return df_bd


 # Função para listar a palavras do dicionário passando as iniciais 
def list_research_dictionary_db(initials,type):
     initials=unidecode.unidecode(initials)
     filter =  " AND   LOWER(unaccent(term)) LIKE \'"+initials+"%\' "
     fetch = " fetch FIRST 10 rows only"
     if initials=="":
        filter ="" 
        fetch="  fetch FIRST 50 rows only"
     filterGraduate_program=""   
    
     #LEFT JOIN graduate_program_researcher gpr ON  r.researcher_id =gpr.researcher_id    
                                 
     sql = """
           SELECT  distinct unaccent(term) as term,count(frequency) as frequency ,type_  
                                from research_dictionary r 
                                

                                 WHERE 
                             
                              
                                  type_='%s'
                                 %s 
                                                             
                                 GROUP BY unaccent(term),type_  ORDER BY frequency desc %s
      """ % (type.upper(),filter,fetch)
     print(sql)
     reg = sgbdSQL.consultar_db(sql)
 

                         
     df_bd = pd.DataFrame(reg, columns=['term','frequency','type'])
     
         

     return df_bd 



def lists_patent_production_researcher_db(researcher_id,year):
      

      sql = """SELECT p.id as id, p.title as title, 
            p.development_year as year, p.grant_date as grant_date
                        
            FROM  patent p
                           where 
                           researcher_id='%s'
                           AND p.development_year::integer>=%s
                           ORDER BY development_year desc""" % (researcher_id,year)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year','grant_date'])
    
      return df_bd

def lists_book_production_researcher_db(researcher_id,year):
      sql = """SELECT b.id as id, b.title as title, 
            b.year as year,bb.isbn,bb.publishing_company
                        
            FROM   bibliographic_production b,bibliographic_production_book bb
                           where 
                           bb.bibliographic_production_id = b.id AND
                           researcher_id='%s'
                           AND b.year_>=%s
                           AND b.type='%s'
                           ORDER BY year_ desc""" % (researcher_id,year,"BOOK")
      print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year','isbn','publishing_company'])

      return df_bd
def lists_book_chapter_production_researcher_db(researcher_id,year):
      sql = """SELECT b.id as id, b.title as title, 
            b.year as year,bc.isbn,bc.publishing_company
                        
            FROM   bibliographic_production b, bibliographic_production_book_chapter bc
                           where 
                           bc.bibliographic_production_id = b.id AND
                           researcher_id='%s'
                           AND b.year_>=%s
                           AND b.type='%s'
                           ORDER BY year_ desc""" % (researcher_id,year,"BOOK_CHAPTER")
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year','isbn','publishing_company'])

      return df_bd
      
def lists_brand_production_researcher_db(researcher_id,year):
      sql = """SELECT b.id as id, b.title as title, 
            b.year as year
                        
            FROM  brand b
                           where 
                           researcher_id='%s'
                           AND b.year>=%s
                           ORDER BY year desc""" % (researcher_id,year)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year'])

      return df_bd

def lists_Researcher_Report_db(researcher_id,year):
      
      sql = """SELECT rr.id as id, rr.title as title, 
            rr.year as year,project_name,financing_institutionc
                        
            FROM  research_report rr
                           where 
                           researcher_id='%s'
                           AND rr.year>=%s
                           ORDER BY year desc""" % (researcher_id,year)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year','project_name','financing_institutionc'])

      return df_bd
      


def lists_guidance_researcher_db(researcher_id,year):
      sql = """SELECT g.id as id, g.title as title, 
                nature,
                oriented,
                type,
                status,
                g.year as year
                        
            FROM  guidance g
                           where 
                           researcher_id='%s'
                           AND g.year>=%s
                           ORDER BY year desc""" % (researcher_id,year)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','nature','oriented','type','status','year'])

      return df_bd
      
      
def lists_software_production_researcher_db(researcher_id,year):
      sql = """SELECT s.id as id, s.title as title, 
            s.year as year
                        
            FROM  software s
                           where 
                           researcher_id='%s'
                           AND s.year>=%s
                           ORDER BY year desc""" % (researcher_id,year)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['id','title','year'])
    
      return df_bd




 
# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def list_researchers_originals_words_db(terms,institution,type,boolean_condition,graduate_program_id):
     print(terms)

     
     
     

    
           
        
     
     filter= util.filterSQLRank(terms,";",boolean_condition,"rf.term","title")

     filterinstitution=""
     filterinstitution= util.filterSQL(institution,";","or","i.name")

     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id 

     print("xxx - "+graduate_program_id)
     if (type=='ARTICLE'):
                          #filter= util.filterSQLRank(terms,";",boolean_condition,"rf.term","title")
                            
                          sql = """SELECT rf.researcher_id as id,COUNT(distinct rf.bibliographic_production_id) AS qtd,
                          r.name as researcher_name,i.name as institution,rp.articles as articles,rp.book_chapters as book_chapters, rp.book as book,
                          r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,abstract,rp.great_area as area,rp.city as city,r.orcid as orcid,i.image as image,
                          r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
                          TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update
                        
                           FROM  researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id , 
                           researcher_frequency rf, institution i, researcher_production rp, city c, bibliographic_production b
                           WHERE 
                            c.id = r.city_id 
                          
                           %s %s %s  
     
                           AND rf.researcher_id = r.id 
                           AND r.institution_id = i.id 
                           AND rp.researcher_id = r.id 
                            AND b.id = rf.bibliographic_production_id
                          

                           GROUP BY rf.researcher_id,r.name, i.name,articles, 
                           book_chapters,book,r.lattes_id,lattes_10_id,abstract, 
                           rp.great_area,rp.city, r.orcid, 
                           i.image,r.graduation,
                           rp.patent,rp.software,rp.brand,TO_CHAR(r.last_update,'dd/mm/yyyy') 
                           ORDER BY qtd desc""" % (filter,filterinstitution,filtergraduate_program)
                          print(sql)
                          reg = sgbdSQL.consultar_db(sql)

                          
     
     if (type=='ABSTRACT'):
                          #filter= util.filterSQLRank(terms,";",boolean_condition,"rf.term","abstract")
                            #AND (translate(unaccent(LOWER(rf.term)),\':\',\'\') ::tsvector@@ \''%s'\'::tsquery)=true
                          sql ="""SELECT distinct rf.researcher_id as id,0 as qtd,
                          r.name as researcher_name,i.name as institution,rp.articles as articles,rp.book_chapters as book_chapters, rp.book as book,
                          r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,abstract,rp.great_area as area,rp.city as city,r.orcid as orcid,i.image as image,
                          r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
                           TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update
                        
                         FROM   researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id , 
                         researcher_abstract_frequency rf, institution i, researcher_production rp, city c
                         WHERE 
                         c.id = r.city_id
                         %s %s 
                         AND rf.researcher_id = r.id
                         AND r.institution_id = i.id 
                         AND rp.researcher_id = r.id 
                         AND (translate(unaccent(LOWER(rf.term)),\':\',\'\') ::tsvector@@ '%s'::tsquery)=true
                       
                          ORDER BY qtd desc """ % (filterinstitution,filtergraduate_program,terms)
                          print(sql)
                          reg = sgbdSQL.consultar_db(sql)

     df_bd = pd.DataFrame(reg, columns=['id','qtd','researcher_name','institution',
                                        'articles','book_chapters','book','lattes',
                                        'lattes_10_id','abstract','area','city','orcid',
                                        'image','graduation','patent','software','brand','lattes_update'])
    
     return df_bd




def lists_bibliographic_production_article_researcher_db(term,researcher_id,year,type,boolean_condition,qualis):
     
     term=unidecode.unidecode(term.lower())
     filter = util.filterSQLRank(term,";",boolean_condition,"rf.term","title")
     filterQualis = util.filterSQL(qualis,";","or","qualis")
     '''
     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

     '''   

      #researcher r   LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id

    
     if (type=='ARTICLE'):
           sql= """ SELECT distinct b.id as id,title,b.year as year,type, doi,qualis, periodical_magazine_name as magazine,
               r.name as researcher,r.lattes_10_id as lattes_10_id,r.lattes_id as lattes_id,jcr as jif,jcr_link 
               FROM bibliographic_production b LEFT JOIN  researcher_frequency rf ON b.id = rf.bibliographic_production_id,
                bibliographic_production_article ba,institution i, researcher r 
                WHERE 
                r.id = b.researcher_id
               
                AND   b.id = ba.bibliographic_production_id 
                 AND r.institution_id=i.id
                AND year_>=%s  %s %s
                AND r.id=\'%s\' order by year desc""" % (year,filter,filterQualis,researcher_id)
           
           reg = sgbdSQL.consultar_db(sql)
           print(sql)
          
     if (type=='ABSTRACT'):

          sql="""
                SELECT distinct b.id as id,title,year,type, doi,qualis, periodical_magazine_name as magazine,r.name as researcher,
                r.lattes_10_id as lattes_10_id,r.lattes_id as lattes_id,jcr as jif,jcr_link
                          FROM bibliographic_production b, researcher_abstract_frequency rf, 
                          bibliographic_production_article ba, researcher r 
                            WHERE  r.id = b.researcher_id
                                   AND rf.researcher_id=r.id 
                                   AND pm.id = ba.periodical_magazine_id 
                                   AND   b.id = ba.bibliographic_production_id 
                                   AND year_ >=%s %s %s
                                   AND r.id='%s'
                                     order by year desc"

          """ % (year,filter,filterQualis,researcher_id)
          reg = sgbdSQL.consultar_db(sql)
         
                        
                        
     
                       # "  AND  b.type = \'ARTICLE\' ")
                       
                        
                        
                     
     df_bd = pd.DataFrame(reg, columns=['id','title','year','type','doi','qualis','magazine','researcher','lattes_10_id','lattes_id','jif','jcr_link'])
   
     return df_bd




def lists_bibliographic_production_qtd_qualis_researcher_db(researcher_id,year,graduate_program_id):
        
     filter=""
     if researcher_id!="":
       filter = " AND b.researcher_id='"+researcher_id+"' "
     filtergraduate_program="";
     if graduate_program_id!="":
       filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

     reg = sgbdSQL.consultar_db(  " SELECT count(*)  as qtd,bar.qualis"+
                                  " FROM  PUBLIC.bibliographic_production b  LEFT JOIN graduate_program_researcher gpr ON  b.researcher_id =gpr.researcher_id,bibliographic_production_article bar,"+
	                               "periodical_magazine pm " + 
                                  "  WHERE "+
                                  "  pm.id = bar.periodical_magazine_id"+
    
                                   " AND   b.id = bar.bibliographic_production_id"+
       

                        "  AND year_ >=%s" % year+
                        filter+
                        " %s " % filtergraduate_program+
                        "  group by bar.qualis order by qualis asc")
                        
                        
     
                       # "  AND  b.type = \'ARTICLE\' ")
                       
                        
                        
                     
     df_bd = pd.DataFrame(reg, columns=['qtd','qualis'])
    
     return df_bd





def lists_word_researcher_db(researcher_id,graduate_program_id):
     
    
    filter="";
    if researcher_id!="":
       filter = "AND r.researcher_id='"+researcher_id+"'"

    filtergraduate_program="";
    if graduate_program_id!="":
       filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id
     
    
    sql =""" SELECT distinct count(unaccent(LOWER(r.term))) AS qtd, unaccent(LOWER(r.term)) as term 
    from researcher_frequency r LEFT JOIN graduate_program_researcher gpr ON  r.researcher_id =gpr.researcher_id 
    WHERE char_length(unaccent(LOWER(r.term)))>3 AND to_tsvector('portuguese', unaccent(LOWER(r.term)))!='' and  unaccent(LOWER(r.term))!='sobre' 
    % s %s group by r.term  ORDER BY qtd DESC fetch FIRST 20 rows only""" % (filter,filtergraduate_program)

    reg = sgbdSQL.consultar_db(sql)
    
        
          
         
                         
                        
                     
    df_bd = pd.DataFrame(reg, columns=['qtd','term'])
    
    return df_bd














'''
 reg = sgbdSQL.consultar_db("SELECT  (term_1 || ' '|| term_2) as term,frequency "+
                        " FROM research_dictionary_bigram WHERE  term_1 <> term_2 AND LOWER(unaccent(term_1)) LIKE \'"+initials+"%\'"+

                        " ORDER BY frequency desc fetch FIRST 10 rows only")

                         
     df_bd = pd.DataFrame(reg, columns=['term','frequency'])
  
     for i,infos in df_bd.iterrows():
        research_dictionary  = {
        'term': str(infos.term),
        'frequency': str(infos.frequency),
         }
        list.append( research_dictionary )
'''

def lista_institution_production_db(text,institution):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')

      
     text =  unidecode.unidecode(text)
     text =  filter= util.filterSQL(text,";","or","rf.term")
     print(text)
     filterinstitution= util.filterSQL(institution,";","or","i.name")
     '''

     filter=""
     if text!="":
      t=[]
      t= text.split(";")  
      filter = ""
      i=0;
      for word in t:
          filter = "LOWER(rf.term)='"+word.lower()+"' or "+ filter
          i=i+1
      x = len(filter)   
      filter = filter[0:x-3]
      filter = " AND ("+filter+")" 
      ''' 
     print(filter)
     reg = sgbdSQL.consultar_db('SELECT COUNT(rf.term) AS qtd,i.id as id, i.name as institution,image'+
   
                        
                         ' FROM researcher_frequency rf, researcher r , institution i, researcher_production rp '+
                          ' WHERE '+
                          ' rf.researcher_id = r.id'
                          ' AND r.institution_id = i.id '+
                          ' AND rp.researcher_id = r.id '+
                          ' AND acronym IS NOT NULL'+
                           '%s'% filter +
                           '%s'% filterinstitution +
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          ' GROUP BY  i.id, i.name ')
     
     df_bd = pd.DataFrame(reg, columns=['qtd','id','institution','image'])
    
     return df_bd



def lista_researcher_id_db(researcher_id):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
 
     sql= """
     SELECT distinct r.id as id,
     r.name as researcher_name,i.name as institution,rp.articles as articles,
     rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,
     r.abstract as abstract,rp.great_area as area,rp.city as city, i.image as image,r.orcid as orcid,
     r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
      TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update 
     FROM  researcher r , city c,  institution i, researcher_production rp WHERE 
                          
       c.id=r.city_id
                                       
     AND r.institution_id = i.id 
     AND rp.researcher_id = r.id 
     AND r.id='%s' 
     """ % researcher_id
     reg = sgbdSQL.consultar_db(sql)
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          #' GROUP BY rf.researcher_id,r.name, i.name,articles, book_chapters,book,r.lattes_id,r.lattes_10_id,r.abstract,gae.name'+
                          #' ORDER BY qtd desc')



     df_bd = pd.DataFrame(reg, columns=['id','researcher_name','institution','articles',
                                        'book_chapters','book','lattes','lattes_10_id','abstract','area','city',
                                        'image','orcid','graduation','patent','software','brand','lattes_update'])
   
     return df_bd


