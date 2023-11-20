
import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util
import Model.Year_Barema as Year_Barema
import Model.Resarcher_Production as Resarcher_Production
 # Função para listar a palavras do dicionário passando as iniciais 


def article_qualis(resarcher_Production,infos):

    print(infos.tipo[7:10])

    
    if(infos.tipo[7:10]=='A1'):
        resarcher_Production.article_A1=infos.qtd
    if(infos.tipo[7:10]=='A2'):
        resarcher_Production.article_A2=infos.qtd
    if(infos.tipo[7:10]=='A3'):
        resarcher_Production.article_A3=infos.qtd    
    if(infos.tipo[7:10]=='A4'):
        resarcher_Production.article_A4=infos.qtd
    if(infos.tipo[7:10]=='B1'):
        resarcher_Production.article_B1=infos.qtd
    if(infos.tipo[7:10]=='B2'):
        resarcher_Production.article_B2=infos.qtd
    if(infos.tipo[7:10]=='B3'):
        resarcher_Production.article_B3=infos.qtd
    if(infos.tipo[7:10]=='B4'):
        resarcher_Production.article_B4=infos.qtd
    if(infos.tipo[7:10]=='SQ'):
        resarcher_Production.article_SQ=infos.qtd
    if(infos.tipo[7:9]=='C'):
        resarcher_Production.article_C=infos.qtd

    return resarcher_Production

def lists_guidance_researcher_db(year,resarcher_Production):
      
      sql = """

            SELECT count(g.id) as qtd,
              
                status,nature
             
                        
            FROM  guidance g
                           where 
                           g.researcher_id='%s' AND
									 
                          
                           g.year>=%s
            GROUP BY      status,nature   


      """ % (resarcher_Production.id,year.resource_completed)
                          #print(sql)

      reg = sgbdSQL.consultar_db(sql)


      df_bd = pd.DataFrame(reg, columns=['qtd','status','nature'])


      for i,infos in df_bd.iterrows():

       

        if util.unidecodelower(infos.nature,"Dissertação De Mestrado") and infos.status=="Concluída":
          
           resarcher_Production.guidance_m_c= infos.qtd

        
        if util.unidecodelower(infos.nature.lower(),"Dissertação De Mestrado") and infos.status=="Em andamento":
          
           resarcher_Production.guidance_m_a= infos.qtd

        if util.unidecodelower(infos.nature,"Tese De Doutorado") and infos.status=="Concluída":
          
           resarcher_Production.guidance_d_c= infos.qtd

        
        if util.unidecodelower(infos.nature,"Tese De Doutorado") and infos.status=="Em andamento":
          
           resarcher_Production.guidance_d_a= infos.qtd



        if (util.unidecodelower(infos.nature,"Iniciacao Cientifica") and infos.status=="Concluída"):
          
           resarcher_Production.guidance_ic_c= infos.qtd
         

        
        if util.unidecodelower(infos.nature,"Iniciacao Cientifica") and infos.status=="Em andamento":
          
           resarcher_Production.guidance_ic_a= infos.qtd



        if util.unidecodelower(infos.nature,"Trabalho de Conclusao de Curso Graduacao") and infos.status=="Concluída":
          
           resarcher_Production.guidance_g_c= infos.qtd

        
        if util.unidecodelower(infos.nature,"Trabalho de Conclusao de Curso Graduacao") and infos.status=="Em andamento":
          
           resarcher_Production.guidance_g_a= infos.qtd    

        if util.unidecodelower(infos.nature,"Monografia de Conclusao de Curso Aperfeicoamento e Especializacao") and infos.status=="Concluída":
          
           resarcher_Production.guidance_e_c= infos.qtd

        
        if util.unidecodelower(infos.nature,"Monografia de Conclusao de Curso Aperfeicoamento e Especializacao") and infos.status=="Em andamento":
          
           resarcher_Production.guidance_e_a= infos.qtd    
       

      #print(df_bd)

      return resarcher_Production

#Função processar e inserir a produção de cada pesquisador
def production_general_db(name,lattes_id,year):
    



  



    
    
    #8933624812566216

    filter=""
    if name=="":
       filter= "r.lattes_id='%s' and " % (lattes_id) 
    else:   
       if name!="todos":
          filter= "r.name='%s' and "  % (name) 


    year_work_event=1900
    sql= """
          
        
         SELECT COUNT(p.id) as qtd, 'PATENT' as type,r.name,r.lattes_10_id,r.graduation,r.id
          FROM patent p,researcher r
          WHERE  p.researcher_id = r.id AND %s p.development_year::INT  >=%s
                group by  type,r.name,r.lattes_10_id,r.graduation,r.id
                UNION                
                
        SELECT COUNT(s.id) as qtd,'SOFTWARE' as type, r.name,r.lattes_10_id,r.graduation,r.id
          from software s,researcher r
          WHERE  s.researcher_id = r.id and %s   s.year >=%s
                 group by  type,r.name,r.lattes_10_id,r.graduation,r.id   
                  UNION                 
  
            SELECT COUNT(ba.id) as qtd,'ARTICLE' || ba.qualis as TYPE , r.name,r.lattes_10_id,r.graduation,r.id
            FROM   PUBLIC.bibliographic_production b , bibliographic_production_article ba, researcher r
			 WHERE  b.id =ba.bibliographic_production_id AND TYPE='ARTICLE' AND b.researcher_id =r.id
			 and %s  b.year_ >=%s
          
                
                 group BY  'ARTICLE' || ba.qualis, r.name,r.lattes_10_id,r.graduation, r.id 
                          
        UNION
        SELECT COUNT(b.id) as qtd, 'BOOK' AS  type ,r.name,r.lattes_10_id,r.graduation,r.id
          FROM   PUBLIC.bibliographic_production b , researcher r
			 where  b.researcher_id =r.id AND TYPE IN ('BOOK')
			 and %s  b.year_ >=%s
			        group BY type, r.name,r.lattes_10_id,r.graduation,r.id     
                      
        UNION          
                
                            
        SELECT COUNT(b.id) as qtd,'BOOK_CHAPTER' as  type ,r.name,r.lattes_10_id,r.graduation,r.id
          FROM   PUBLIC.bibliographic_production b , researcher r
			 where  b.researcher_id =r.id AND TYPE IN ('BOOK_CHAPTER') 
			 and %s   b.year_ >=%s
          
                
                 group BY type, r.name,r.lattes_10_id,r.graduation,r.id   
                  UNION 
        SELECT COUNT(b.id) as qtd,'WORK_IN_EVENT' as type ,r.name,r.lattes_10_id,r.graduation,r.id
          FROM   PUBLIC.bibliographic_production b , researcher r
			 where  b.researcher_id =r.id AND TYPE IN ( 'WORK_IN_EVENT') 
			 and %s  b.year_ >=%s
          
                
                 group BY type, r.name,r.lattes_10_id,r.graduation,r.id  
             UNION    
            SELECT COUNT(b.id) as qtd,'EVENT_ORGANIZATION' as type ,r.name,r.lattes_10_id,r.graduation,r.id
          FROM   event_organization b , researcher r
			 where  b.researcher_id =r.id 
			 and %s   b.year >=%s
          
                
                 group BY type, r.name,r.lattes_10_id,r.graduation,r.id      
                  UNION
                   SELECT COUNT(b.id) as qtd,'PARTICIPATION_EVENTS' as type ,r.name,r.lattes_10_id,r.graduation,r.id
          FROM   participation_events b , researcher r
			 where  b.researcher_id =r.id 
			 and %s   b.year >=%s
          
                
                 group BY type, r.name,r.lattes_10_id,r.graduation,r.id     
                
                
        

    """ % (filter,year.patent,filter,year.sofware,filter,year.article,
           filter,year.book,filter,year.chaper_book,filter,year.work_event,filter,year.participation_events,filter,year.participation_events)
    print(sql)
    reg = sgbdSQL.consultar_db(sql)
    df_bd = pd.DataFrame(reg, columns=['qtd','tipo','name_','lattes_10_id','graduation','researcher_id'])
   

    list_Resarcher_Production=[]
    resarcher_Production = Resarcher_Production.Resarcher_Production()




   
 
    for i,infos in df_bd.iterrows():

        #print(infos.tipo)
        #print(infos.qtd)
        resarcher_Production.lattes_10_id = infos.lattes_10_id
        resarcher_Production.researcher = str(infos.name_)
        resarcher_Production.id = str(infos.researcher_id)
        
        resarcher_Production.graduation = infos.graduation
        

        if infos.tipo=="BOOK":
          
           resarcher_Production.book= infos.qtd
           

        if infos.tipo=="WORK_IN_EVENT":
            resarcher_Production.work_in_event= infos.qtd   

        if infos.tipo[0:7]=="ARTICLE":
            #print(infos.tipo[7:10])
            resarcher_Production = article_qualis(resarcher_Production,infos)
            #resarcher_Production.article = infos.qtd   

        if infos.tipo=="BOOK_CHAPTER":
           resarcher_Production.book_chapter = infos.qtd   
        if infos.tipo=="PATENT":
            resarcher_Production.patent = infos.qtd       
        if infos.tipo=="SOFTWARE":
            resarcher_Production. software = infos.qtd  
        if infos.tipo=="BRAND":
            resarcher_Production.brand = infos.qtd  
        if infos.tipo=="EVENT_ORGANIZATION":
            resarcher_Production.event_organization = infos.qtd  
        if infos.tipo=="PARTICIPATION_EVENTS":
            resarcher_Production.participation_event = infos.qtd   


    lists_guidance_researcher_db(year,resarcher_Production)
    return resarcher_Production.getJson()



def researcher_production_db(list_name,list_resarcher_lattes_id,year):
   
   
              
  

    
    
     


      
    if list_resarcher_lattes_id!="":
      t=[]
      t= list_resarcher_lattes_id.split(";")  

      list_resarcher=[]
     
      i=0;
      for word in t:
          
          list_resarcher.append(production_general_db("",word,year))
          i=i+1
     
      return list_resarcher  

    else:
      


      if list_name!="todos":
        t=[]
        t= list_name.split(";")  

        list_resarcher=[]
     
        i=0;
        for word in t:
          
            list_resarcher.append(production_general_db(word,"",year))
            i=i+1
     
        return list_resarcher
      else:
           sql="select name from researcher "
           reg = sgbdSQL.consultar_db(sql)
           list_resarcher=[]
           x=0
           df_bd = pd.DataFrame(reg, columns=['name_'])
           for i,infos in df_bd.iterrows():
                list_resarcher.append(production_general_db(infos.name_,"",year))
                #i=i+1
                x=x+1
                print(str(x))
     
           return list_resarcher

          


    
