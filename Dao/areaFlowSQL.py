
import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util
 # Função para listar a palavras do dicionário passando as iniciais 




# Função que lista  as áreas de expertrize por Iniciais

def lists_great_area_expertise_researcher_db(researcher_id):
     
     reg = sgbdSQL.consultar_db("SELECT distinct gae.name as area from great_area_expertise gae, researcher_area_expertise r "
                                 "  WHERE "
                                 " gae.id =r.great_area_expertise_id "

                                 "  AND r.researcher_id='%s'" % researcher_id)
                         
     df_bd = pd.DataFrame(reg, columns=['area'])
     area=""
     for i,infos in df_bd.iterrows():
          area=infos.area+" ; "+area  

     
     x = len(area)   
     area = area[0:x-2]
     return area

def lists_area_speciality_researcher_db(researcher_id):
     
     reg = sgbdSQL.consultar_db("SELECT distinct asp.name as specialty, ae.name as area_expertise  "+
                               "  from area_expertise ae,area_specialty asp, researcher_area_expertise r "
                                 "  WHERE "
                                 " asp.id =r.area_specialty_id "
                                 " AND r.area_expertise_id = ae.id"

                                 "  AND r.researcher_id='%s'" % researcher_id)
                         
     df_bd = pd.DataFrame(reg, columns=['specialty','area_expertise'])
     area=""
     for i,infos in df_bd.iterrows():
          area=infos.specialty + " | "+infos.area_expertise+" ; "+area  

     
     x = len(area)   
     area = area[0:x-2]
     return area

def lists_great_area_expertise_term_initials_db(initials):
     
     initials=unidecode.unidecode(initials)
   
     reg = sgbdSQL.consultar_db("SELECT id, name as nome from great_area_expertise WHERE   LOWER(unaccent(name)) LIKE \'"+initials+"%\' order by nome")
                         
     df_bd = pd.DataFrame(reg, columns=['id','nome'])
     
     print(df_bd)
     return df_bd

def lists_area_speciality_term_initials_db(initials,area,graduate_program_id):

     area = area.replace(" ","_")
     filter= util.filterSQL(area.lower(),",","or","gea.name")
     
     initials=unidecode.unidecode(initials)


     initials=unidecode.unidecode(initials)
     t= initials.split(";")
     filter_=""
     for word in t:
         filter_= "(        translate(unaccent(LOWER(asp.name)),\':;\',\'\') ::tsvector@@ unaccent(LOWER('"+word.lower()+"'))::tsquery)=TRUE  and"+ filter_
         
     x = len(filter_)   
     filter_ = filter_[0:x-3]
     filter_ = "("+filter_+")" 
     fetch=""
     if filter_=="":
          fetch="  fetch FIRST 30 rows only"
     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

     reg = sgbdSQL.consultar_db(" SELECT  distinct ae.name as area_expertise, asp.name as area_specialty "+
                                 " FROM area_expertise ae, area_specialty asp, great_area_expertise gea, "
                                 " researcher_area_expertise rae  LEFT JOIN graduate_program_researcher gpr ON  rae.researcher_id =gpr.researcher_id  "+

                                 " WHERE "+
                                 " gea.id = rae.great_area_expertise_id "+
                                 " AND ae.id = rae.area_expertise_id "+
                                 " AND asp.id = rae.area_specialty_id "
                                   " AND ( LOWER(unaccent(asp.name)) LIKE \'"+initials.lower()+"%' )"+
                                  #  "  or " +filter_+")"+
                                " %s" % filter+
                                " %s " % filtergraduate_program+
                             
                                #" AND  LOWER(gea.name)='%s'" % area.lower()+
                                " ORDER BY  ae.name,asp.name %s" % fetch )
     '''
     reg = sgbdSQL.consultar_db("SELECT asp.id as id,gea.name as great_area ,ae.name as area_expertise ,sue.name as  sub_area_expertise,asp.name as area_specialty"+
                                " FROM area_expertise ae, area_specialty asp, great_area_expertise gea, sub_area_expertise sue "+

                                " WHERE "+
                                " ae.great_area_expertise_id = gea.id "+ 
                                " AND sue.area_expertise_id = ae.id "+
                                " AND asp.sub_area_expertise_id=sue.id "+
                                " AND LOWER(unaccent(asp.name)) LIKE \'"+initials.lower()+"%\' "+
                                "%s" % filter+
                                #" AND  LOWER(gea.name)='%s'" % area.lower()+
                                " ORDER BY  gea.name,ae.name,sue.name,asp.name ")
      '''                          
                                
                                
                                
                         
     df_bd = pd.DataFrame(reg, columns=['area_expertise' ,'area_specialty'])
     
     print(df_bd)
     return df_bd

def lista_researcher_area_expertise_db(text,institution):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     text = text.replace(" ","_")
     filter= util.filterSQL(text,";","or","gae.name")
     

     filterinstitution= util.filterSQL(institution,";","or","i.name")
    
    
     if (filter!=" "): 
          reg = sgbdSQL.consultar_db("SELECT distinct rp.great_area as area,r.id as id,"+
                        "r.name as researcher_name,i.name as institution,rp.articles as articles,"+
                        " rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,"+
                        "r.orcid as orcid,rp.city as city, i.image as image"
                         ' FROM  researcher r , institution i, researcher_production rp,researcher_area_expertise re, great_area_expertise gae, city c '+
                          ' WHERE '+
                         
                          '  re.researcher_id =r.id'
                          ' And r.city_id=c.id'
                          ' AND gae.id = re.great_area_expertise_id'
                          ' AND r.institution_id = i.id '+
                          ' AND rp.researcher_id = r.id '+
                          "%s" % filter +
                          "%s" % filterinstitution+
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          ' ORDER BY researcher_name')

     else:                    
          reg = sgbdSQL.consultar_db("SELECT distinct rp.great_area as area,r.id as id,"+
                        "r.name as researcher_name,i.name as institution,rp.articles as articles,"+
                        " rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,"+
                        "r.orcid as orcid,rp.city as city, i.image as image"
                         ' FROM  researcher r , institution i, researcher_production rp,city c '+
                          ' WHERE '+
                         
                       #   '  re.researcher_id =r.id'
                          ' r.city_id=c.id'
                         # ' AND gae.id = re.great_area_expertise_id'
                          ' AND r.institution_id = i.id '+
                          ' AND rp.researcher_id = r.id '+
                          "%s" % filter +
                          "%s" % filterinstitution+
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          ' ORDER BY researcher_name')



     df_bd = pd.DataFrame(reg, columns=['area','id','researcher_name','institution','articles','book_chapters','book','lattes','lattes_10_id','abstract','orcid','city','image'])
    #print (df_bd)
     return df_bd

def lista_production_article_area_expertise_db(great_area_expertise, area_specialty,year,qualis,graduate_program_id):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')



     great_area_expertise = great_area_expertise.replace(" ","_")
     filter= util.filterSQL(great_area_expertise,";","or","rp.great_area_expertise")
     #filter_specialty = util.filterSQL(area_specialty,";","or","asp.name")

     print(area_specialty)
     area_specialty = area_specialty.replace("&"," ")
     area_specialty = unidecode.unidecode(area_specialty.lower())

     filter_specialty = util.filterSQLLike(area_specialty,";","or","rp.area_specialty")
    
     print("ssss"+filter_specialty)

     filterQualis = util.filterSQL(qualis,";","or","qualis")

    
     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id

  

      


     

     reg = sgbdSQL.consultar_db("SELECT distinct bp.id as id ,bp.title as title,r.name as researcher,"
                        "r.lattes_id as lattes_id,lattes_10_id,rp.great_area as area,bp.year as year,pm.name as magazine, doi,qualis,jcr,jcr_link"+
                        " FROM  researcher r  LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id,"
                       " researcher_production rp, "+
	                   "bibliographic_production bp,bibliographic_production_article bar,"+
	                  "periodical_magazine pm " +
                        " WHERE  "+

                         " pm.id = bar.periodical_magazine_id"+
                         #" AND asp.id= re. area_specialty_id"
    
                         " AND   bp.id = bar.bibliographic_production_id"+
                         " AND bp.researcher_id = r.id "+                
                         " AND rp.researcher_id =r.id "+
                         #" AND gae.id = re.great_area_expertise_id "+
                         "%s" % filter+
                         "%s" % filterQualis+
                         "%s" % filter_specialty +
                         "%s " % filtergraduate_program +
                         "  AND year_ >=%s" % year+
                          " Group by bp.id  ,bp.title ,r.name ,"+
                         "r.lattes_id, lattes_10_id,great_area ,bp.year,pm.name , doi,qualis")


     df_bd = pd.DataFrame(reg, columns=['id','title','researcher','lattes_id','lattes_10_id','area','year','magazine','doi','qualis','jcr','jcr_link'])
     print (df_bd)
     return df_bd

def lista_institution_area_expertise_db(great_area,area_specialty,institution):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
    
     great_area = great_area.replace(" ","_")
     filter_great_area= util.filterSQL(great_area,";","or","rp.great_area")
     #filter_area_specialty = util.filterSQL(area_specialty,";","or","asp.name")
     print(area_specialty)
     area_specialty = area_specialty.replace("&"," ")
     area_specialty = unidecode.unidecode(area_specialty.lower())

     filter_specialty = util.filterSQLLike(area_specialty,";","or","rp.area_specialty")

       
  

     filterinstitution=util.filterSQL(institution,";","or","i.name")
     if (filterinstitution==" "):
         filterinstitution = " AND i.name in ('Universidade do Estado da Bahia','Universidade Estadual de Feira de Santana','Universidade Estadual de Santa Cruz','Universidade Estadual do Sudoeste da Bahia' )"

     reg = sgbdSQL.consultar_db('SELECT i.id as id,i.image as image,'+
                                'i.name as institution,count(r.id) as qtd'+
                                ' FROM  researcher r , institution i,researcher_production rp  '+
                                ' WHERE '+
                                ' rp.researcher_id =r.id'
                                
                                ' AND r.institution_id = i.id '+      

                                "%s" % filter_great_area +
                                "%s" % filter_specialty +
                                "%s" % filterinstitution+
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                              ' Group by i.id ,i.image,'+
                              'i.name '+
                              ' ORDER BY i.name')



     df_bd = pd.DataFrame(reg, columns=['id','image','institution','qtd'])
     print (df_bd)
     return df_bd

def lista_researcher_area_speciality_db(text,institution,graduate_program_id):
    
   
     #reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
      #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')
     print(text)
     text = text.replace("&"," ")
     text = unidecode.unidecode(text.lower())

     filter = util.filterSQLLike(text,";","or","rp.area_specialty")
     #filter= util.filterSQL(text,";","or","gae.name")
     

     filterinstitution= util.filterSQL(institution,";","or","i.name")
     print("XXXXXXXXXXXXXXXXXXXXX" +text)
     print(filterinstitution)


     filtergraduate_program=""
     if graduate_program_id!="":
        filtergraduate_program = "AND gpr.graduate_program_id="+graduate_program_id
    

     reg = sgbdSQL.consultar_db("SELECT distinct rp.great_area as area,rp.area_specialty as area_specialty,r.id as id,"+
                        "r.name as researcher_name,i.name as institution,rp.articles as articles,"+
                        " rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,"+
                        "r.orcid as orcid,rp.city  as city, i.image as image"
                         " FROM  researcher r  LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id "
                         ", institution i, researcher_production rp,researcher_area_expertise re, great_area_expertise gae, city c "+
                          ' WHERE '+
                         
                          '  re.researcher_id =r.id'
                          ' And r.city_id=c.id'
                          ' AND gae.id = re.great_area_expertise_id'
                          ' AND r.institution_id = i.id '+
                          ' AND rp.researcher_id = r.id '+

                         # ' AND (translate(unaccent(LOWER(rp.area_specialty)),\':\',\'\')::tsvector@@ \''+text.lower()+'\'::tsquery)=true '
                       #  " AND unaccent(LOWER(rp.area_specialty)) LIKE '%"+text+"%' "+
                          "%s" % filter +
                          "%s" % filterinstitution+
                          "%s" % filtergraduate_program +
                          #' AND term = \''+term+"\'"
                          #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
                          ' ORDER BY researcher_name')

     


     df_bd = pd.DataFrame(reg, columns=['area','area_specialty','id','researcher_name','institution','articles','book_chapters','book','lattes','lattes_10_id','abstract','orcid','city','image'])
     print (df_bd)
     return df_bd


#lists_area_speciality_term_initials_db("Mo","ciencias_exatas_e_da_terra")
