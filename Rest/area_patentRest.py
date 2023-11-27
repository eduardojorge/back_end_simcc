from flask import Flask, jsonify, request,Blueprint

import json
#import nltk
import unidecode
from flask_cors import CORS,cross_origin
import os

import Dao.areaFlowSQL as areaFlowSQL
import Dao.termFlowSQL as termFlowSQL

from Model.Researcher import Researcher
from Model.Bibliographic_Production_Researcher import Bibliographic_Production_Researcher


##from server import app

#https://www.fullstackpython.com/flask-json-jsonify-examples.html
#app = Flask(__name__)
#app.config["CORS_HEADERS"] = "Content-Type"
#app.route('/')
#CORS(app, resources={r"/*":{"origins":"*"}})
#app = Flask(__name__)

#if __name__ == '__main__': app.run(host='192.168.15.69',port=5000)
areaRest = Blueprint('areaRest', __name__)


######### Patent 

@areaRest.route('/researcherPatent', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherPatent():
    list_researcher_area_expertise  = []
    
    term = request.args.get('term')
    if term=="":
        return
    #stemmer = nltk.RSLPStemmer()

    graduate_program_id =request.args.get('graduate_program_id')
    if graduate_program_id is None:
        graduate_program_id =""
    
  
    #termNovo=unidecode.unidecode(name.replace(";","&"))

    university=""
    university = str(request.args.get('university'))+""
    
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    #df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd =areaFlowSQL.lista_researcher_patent_db(term.lower(),university,graduate_program_id)
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        #area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        #area_=" "
        researcher  = {
        'id': str(infos.id),
        'name': str(infos.researcher_name),
        'articles':str(infos.articles),
        'book_chapters':str(infos.book_chapters),
        'book':str(infos.book),
        'university':str(infos.institution),
        'lattes_id':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id),
        'area':str(infos.area.replace("_"," ")),
        'abstract':str(infos.abstract),   
        'city':str(infos.city),
        'orcid':str(infos.orcid),
        'image':str(infos.image),
        'area_specialty':str(infos.area_specialty)

        }
        #print(researcher)
        list_researcher_area_expertise.append(researcher) 
    return jsonify(list_researcher_area_expertise), 200


######### Fluxo Area

#print(list_originals_words_initials_term_db("rob")) 
@areaRest.route('/area_expertiseInitials', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def area_expertiseInitials():
    list_area_expertise  = []
    initials = request.args.get('initials')

    df_bd = areaFlowSQL.lists_great_area_expertise_term_initials_db(initials.lower())

    for i,infos in df_bd.iterrows():
        print(infos.nome)
        researcher  = {
        'id': str(infos.id),
        'name': infos.nome.replace("_"," "),
     
        }
        #print(researcher)
        list_area_expertise.append(researcher) 

    return jsonify(list_area_expertise),200


@areaRest.route('/area_specialitInitials', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def area_specialitInitials():
    list_area_specialit  = []
    initials = request.args.get('initials')
    area = request.args.get('area')

    graduate_program_id =request.args.get('graduate_program_id')
    #print("yyyyy "+graduate_program_id  )
    if graduate_program_id is None:
        graduate_program_id =""

    df_bd = areaFlowSQL.lists_area_speciality_term_initials_db(initials,area,graduate_program_id)

    for i,infos in df_bd.iterrows():
  
        area_specialit_  = {
       # 'id': str(infos.id),
      #  'great_area':str(infos.great_area.replace("_"," ")) ,
        'area_expertise':str(infos.area_expertise),
        #'sub_area_expertise':str(infos.sub_area_expertise),
        'area_specialty':str(infos.area_specialty)
       
        }
        #print(researcher)
        list_area_specialit.append(area_specialit_) 

    return jsonify(list_area_specialit),200

@areaRest.route('/researcherArea_expertise', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherArea_expertise():
    list_researcher_area_expertise  = []
    area = request.args.get('area')
    #stemmer = nltk.RSLPStemmer()
  
    #termNovo=unidecode.unidecode(name.replace(";","&"))

    university=""
    university = str(request.args.get('university'))+""
    
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    #df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd =areaFlowSQL.lista_researcher_area_expertise_db(area.lower(),university)
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        #area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        #area_=" "
        researcher  = {
        'id': str(infos.id),
        'name': str(infos.researcher_name),
        'articles':str(infos.articles),
        'book_chapters':str(infos.book_chapters),
        'book':str(infos.book),
        'university':str(infos.institution),
        'lattes_id':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id),
        'area':str(infos.area_.replace("_"," ")),
        'abstract':str(infos.abstract),   
        'city':str(infos.city),
        'orcid':str(infos.orcid),
        'image':str(infos.image)

        }
        #print(researcher)
        list_researcher_area_expertise.append(researcher) 
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route('/researcherArea_specialty', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherArea_specialty():
    list_researcher_area_expertise  = []
    
    area = request.args.get('area_specialty')
    if area=="":
        return
    #stemmer = nltk.RSLPStemmer()

    graduate_program_id =request.args.get('graduate_program_id')
    if graduate_program_id is None:
        graduate_program_id =""
    
  
    #termNovo=unidecode.unidecode(name.replace(";","&"))

    university=""
    university = str(request.args.get('university'))+""
    
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    #df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd =areaFlowSQL.lista_researcher_area_speciality_db(area.lower(),university,graduate_program_id)
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        #area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        #area_=" "
        researcher  = {
        'id': str(infos.id),
        'name': str(infos.researcher_name),
        'articles':str(infos.articles),
        'book_chapters':str(infos.book_chapters),
        'book':str(infos.book),
        'university':str(infos.institution),
        'lattes_id':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id),
        'area':str(infos.area.replace("_"," ")),
        'abstract':str(infos.abstract),   
        'city':str(infos.city),
        'orcid':str(infos.orcid),
        'image':str(infos.image),
        'area_specialty':str(infos.area_specialty)

        }
        #print(researcher)
        list_researcher_area_expertise.append(researcher) 
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route('/bibliographic_production_article_area', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def bibliographic_production_article_area():
    list_bibliographic_production_article  = []
    great_area = request.args.get('great_area')
    area_specialty = request.args.get('area_specialty')
    year = request.args.get('year')
    qualis = request.args.get('qualis')

    #stemmer = nltk.RSLPStemmer()
  
    great_area=unidecode.unidecode(great_area.lower())
    area_specialty=unidecode.unidecode(area_specialty.lower())

    
    graduate_program_id =request.args.get('graduate_program_id')
    #print("yyyyy "+graduate_program_id  )
    if graduate_program_id is None:
        graduate_program_id =""
   
  
    #terms = unidecode(terms.lower())
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    df_bd =areaFlowSQL.lista_production_article_area_expertise_db(great_area,area_specialty,year,qualis,graduate_program_id )

   # 'bp.title','ba.res_name','r.lattes_id','area','year','pm.name','doi','qualis'

   
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        bibliographic_production_article_  = {
         'id': str(infos.id),   
        'title': str(infos.title),
        'researcher': str(infos.researcher),
        'lattes_id':str(infos.lattes_id),
         'lattes_10_id':str(infos.lattes_10_id),
        'area:':str(infos.area),
        'year': str(infos.year),
        'doi': str(infos.doi),
        'qualis':str(infos.qualis),
        'magazine':str(infos.magazine),
          'jif':str(infos.jcr),
            'jcr_link':str(infos.jcr_link) 

       
        

        }
        #print(researcher)
        list_bibliographic_production_article.append(bibliographic_production_article_) 

    return jsonify(list_bibliographic_production_article), 200


@areaRest.route('/institutionArea', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])
def institutionArea():
    list_institutionFrequenci  = []
    great_area = request.args.get('great_area')
    area_specialty = request.args.get('area_specialty')
    

    #stemmer = nltk.RSLPStemmer()
  
    great_area=unidecode.unidecode(great_area.lower())
    area_specialty=unidecode.unidecode(area_specialty.lower())

   
   
    university=""
    university = str(request.args.get('university'))+""
    #terms = unidecode(terms.lower())
    #print(termNovo)
   # print(stemmer.stem(termNovo))
    df_bd =areaFlowSQL.lista_institution_area_expertise_db(great_area,area_specialty,university)
    #df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i,infos in df_bd.iterrows():
        institution  = {
        'id': str(infos.id),
        'institution': str(infos.institution),
        'among': str(infos.qtd),
        'image':str(infos.image)
       

        }
        #print(researcher)
        list_institutionFrequenci.append(institution) 

    return jsonify(list_institutionFrequenci), 200

##############################################################################

if __name__ == '__main__':
    # run app in debug mode on port 5000
    areaRest.run(debug=True, port=5001, host='0.0.0.0')