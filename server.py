from flask import Flask, jsonify, request
import json
import nltk
import unidecode
from flask_cors import CORS,cross_origin
import os

#import DaoPostgres.termFlow as termFlow

from Model.Researcher import Researcher
from Model.Bibliographic_Production_Researcher import Bibliographic_Production_Researcher
from Model.Magazine import Magazine

from Rest.researcherTermRest import researcherTermRest
from Rest.areaRest import areaRest
from Rest.cimatecRest import cimatecRest
import SimccBD as SimccBD
import Dao.areaFlowSQL
import Dao.generalSQL 


#from  Rest.researcherTermRest import researcherTermRest



#https://www.fullstackpython.com/flask-json-jsonify-examples.html
app = Flask(__name__)
app.register_blueprint(researcherTermRest)
app.register_blueprint(areaRest)
app.register_blueprint(cimatecRest)
app.config["CORS_HEADERS"] = "Content-Type"
app.route('/')
CORS(app, resources={r"/*":{"origins":"*"}})
#app = Flask(__name__)

#if __name__ == '__main__': app.run(host='192.168.15.69',port=5000)


#lists_bibliographic_production_article_researcher_db("Robótica",'35e6c140-7fbb-4298-b301-c5348725c467')
@app.route('/magazine', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def patent_production_researcher():
    list_magazine = []
    #terms = request.args.get('terms')
    magazine_initialis = request.args.get('initials')
    issn = request.args.get('issn')
    
   

   

   
    df_bd =Dao.generalSQL.lists_magazine_db(magazine_initialis,issn)

    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():

        m = Magazine()
        m.id =  str(infos.id)
        m.magazine = str(infos.magazine)
        m.issn =  str(infos.issn)
        m.jif = str(infos.jcr)
        m.jcr_link = str(infos.jcr_link)
        m.qualis = str(infos.qualis)


        #print(researcher)
        list_magazine.append(m.getJson()) 
     


 
    return jsonify(list_magazine), 200


#print(list_originals_words_initials_term_db("rob")) 
@app.route('/reasercherInitials', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def reasercherInitials():
    print("teste")
    list_researcher  = []
    initials = request.args.get('initials')
    print(initials)
  
    graduate_program_id =request.args.get('graduate_program_id')
    if graduate_program_id is None:
        graduate_program_id =""
    

    

    df_bd = SimccBD.lists_researcher_initials_term_db(initials.lower(), graduate_program_id)



    for i,infos in df_bd.iterrows():
        #print(infos.nome)
        researcher  = {
        'id': str(infos.id),
        'name': infos.nome,
     
        }
        #print(researcher)
        list_researcher.append(researcher) 

    return jsonify(list_researcher),200



#print(list_originals_words_initials_term_db("rob")) 




@app.route('/researcher_image', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def researcher_image():
    
  
    df_bd =SimccBD.lista_researcher_full_name_db()
    list_iamge=[]

  
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        researcher_image  = {
       

        'id': str(infos.id),
      
        'lattes':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id)

        }
        #print(researcher)
        list_iamge.append(researcher_image) 

    return jsonify(list_iamge), 200
    
#print(list_researchers_originals_words_db("robótica | robô | robotics"))   
@app.route('/researcherName', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherName():
    list_researcher  = []
    name = request.args.get('name')
    stemmer = nltk.RSLPStemmer()
 
    #termNovo=unidecode.unidecode(name.replace(";","&"))
   
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    #df_bd =SimccBD.lista_researcher_name_db(name.lower())
    graduate_program_id =request.args.get('graduate_program_id')
    if graduate_program_id is None:
        graduate_program_id =""

    df_bd =SimccBD.lista_researcher_full_name_db_(name.lower(),graduate_program_id)
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        #area = Dao.areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)

        r = Researcher()
        r.id = str(infos.id)
        r.name = str(infos.researcher_name)
        r.among = str("0")
        r.articles  =str(infos.articles)
        r.book_chapters =str(infos.book_chapters)
        r.book =str(infos.book)
        r.university =str(infos.institution)
        r.lattes_id = str(infos.lattes)
        r.lattes_10_id =str(infos.lattes_10_id)
        r.abstract =str(infos.abstract)
        r.area =str(infos.area.replace("_"," "))
        r.city= str(infos.city)
        r.orcid =str(infos.orcid)
        r.patent = str(infos.patent)
        r.image_university =str(infos.image)
        r.software = str(infos.software)
        r.brand = str(infos.brand)
        r.university =str(infos.institution)
        r.graduation = str(infos.graduation)
        r.lattes_update = str(infos.lattes_update)


        print(r.getJson())
        
        researcher = r.getJson()

        #print(researcher)
        list_researcher.append(researcher) 
    return jsonify(list_researcher), 200













@app.route('/total', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def total():

    researcher_total = SimccBD.researcher_total_db()
    institution_total = SimccBD.institution_total_db()
    bibliographic_production_total = SimccBD.bibliographic_production_total_db()

    print(researcher_total)


    total_ = [{
    "researcher": str(researcher_total),
    "publications": str(bibliographic_production_total),
    "organizations": str(institution_total) 

    }]
   
   


    
    return jsonify(total_), 200






@app.route('/bibliographic_production_article', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def bibliographic_production_article():
    list_bibliographic_production_article  = []
    terms = request.args.get('terms')
    year = request.args.get('year')
    qualis = request.args.get('qualis')
    university=""
    university = str(request.args.get('university'))+""
    distinct = str(request.args.get('distinct'))+""

    graduate_program_id =request.args.get('graduate_program_id')
    #print("yyyyy "+graduate_program_id  )
    if graduate_program_id is None:
        graduate_program_id =""
   


    #stemmer = nltk.RSLPStemmer()
    print(terms)
    termNovo=unidecode.unidecode(terms.lower())
    print(terms)
    #terms = unidecode(terms.lower())
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    df_bd =SimccBD.lists_bibliographic_production_article_db(termNovo,year,qualis,university,distinct,graduate_program_id)

    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    print(distinct)
    print(df_bd)
    for i,infos in df_bd.iterrows():

        if (distinct=="0"):
            bibliographic_production_article_  = {
          
            'title': str(infos.title),
            'year': str(infos.year),
            'doi': str(infos.doi),
            'qualis':str(infos.qualis),
            'name_periodical':str(infos.magazine),
            'researcher':str(infos.researcher),
            'lattes_10_id':str(infos.lattes_10_id),
            'lattes_id':str(infos.lattes_id),
            'jif':str(infos.jcr),
            'jcr_link':str(infos.jcr_link) 
            }

        if (distinct=="1"):
        
            bibliographic_production_article_  = {
          
            'title': str(infos.title),
            'year': str(infos.year),
            'doi': str(infos.doi),
            'qualis':str(infos.qualis),
            'name_periodical':str(infos.magazine),
            'jif':str(infos.jcr),
            'jcr_link':str(infos.jcr_link) 
           # 'researcher':str(""),
           # 'lattes_10_id':str(""),
          #  'lattes_id':str("")
            }


        

        
        #print(researcher)
        list_bibliographic_production_article.append(bibliographic_production_article_) 
     


 
    return jsonify(list_bibliographic_production_article), 200





















#if __name__ == "__main__":
#    app.run(debug=True)
if __name__ == '__main__':
    # run app in debug mode on port 5000
   # context = ('cert.pem', 'local.key')#certificate and key files
    app.run(debug=True, port=5001, host='0.0.0.0')
