from flask import Flask, jsonify, request,Blueprint

import json
#import nltk
import unidecode
from flask_cors import CORS,cross_origin
import os

import Dao.graduate_programSQL as graduate_programSQL


from Model.GraduateProgram  import GraduateProgram 
from Model.Bibliographic_Production_Researcher import Bibliographic_Production_Researcher


##from server import app

#https://www.fullstackpython.com/flask-json-jsonify-examples.html
#app = Flask(__name__)
#app.config["CORS_HEADERS"] = "Content-Type"
#app.route('/')
#CORS(app, resources={r"/*":{"origins":"*"}})
#app = Flask(__name__)

#if __name__ == '__main__': app.run(host='192.168.15.69',port=5000)
graduateProgramRest = Blueprint('graduateProgramRest', __name__)




######### Fluxo Area

#print(list_originals_words_initials_term_db("rob")) 
@graduateProgramRest.route('/graduate_program', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])

def graduate_program():

    list_gradute_program =[]
    institution_id =request.args.get('institution_id')
    #print("yyyyy "+graduate_program_id  )
    if institution_id is None:
        institution_id =""
    df_bd = graduate_programSQL.graduate_program_db(institution_id)
    for i,infos in df_bd.iterrows():
        graduateProgram = GraduateProgram()
        graduateProgram.graduate_program_id = str(infos.graduate_program_id)
        graduateProgram.code = str(infos.code)
        graduateProgram.name = str(infos.program)
        graduateProgram.area = str(infos.area.strip())
        graduateProgram.modality = str(infos.modality)
        graduateProgram.type = str(infos.type)
        graduateProgram.rating = str(infos.rating)

      

        list_gradute_program.append(graduateProgram.getJson()) 

    return jsonify(list_gradute_program),200




#print(list_originals_words_initials_term_db("rob")) 
@graduateProgramRest.route('/graduate_program_profnit', methods=['GET'])
@cross_origin(origin="*", headers=["Content-Type"])

def graduate_program_profnit():

    list_gradute_program =[]
    #institution_id =request.args.get('institution_id')
    #print("yyyyy "+graduate_program_id  )
    #if institution_id is None:
       # institution_id =""
    df_bd = graduate_programSQL.graduate_program_profnit_db()
    for i,infos in df_bd.iterrows():
        graduateProgram = GraduateProgram()
        graduateProgram.graduate_program_id = str(infos.graduate_program_id)
        graduateProgram.code = str(infos.code)
        graduateProgram.name = str(infos.program)
        graduateProgram.area = str(infos.area.strip())
        graduateProgram.modality = str(infos.modality)
        graduateProgram.type = str(infos.type)
        graduateProgram.rating = str(infos.rating)
        graduateProgram.state = str(infos.state)
        graduateProgram.city =str(infos.city)
        graduateProgram.instituicao = str(infos.instituicao)
        graduateProgram.url_image = str(infos.url_image)
        graduateProgram.region = str(infos.region)
        graduateProgram.latitude = str(infos.latitude)
        graduateProgram.longitude = infos.longitude
        graduateProgram.sigla = infos.sigla



      

        list_gradute_program.append(graduateProgram.getJson()) 

    return jsonify(list_gradute_program),200






##############################################################################

if __name__ == '__main__':
    # run app in debug mode on port 5000
    cimatecRest.run(debug=True, port=5001, host='0.0.0.0')