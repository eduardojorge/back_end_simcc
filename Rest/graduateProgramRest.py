from flask import Blueprint, jsonify, request

import Dao.graduate_programSQL as graduate_programSQL
from Model.GraduateProgram import GraduateProgram

graduateProgramRest = Blueprint("graduateProgramRest", __name__)


@graduateProgramRest.route("/graduate_program_production", methods=["GET"])
def graduate_program_production():
    graduate_program_id = request.args.get("graduate_program_id")
    year = request.args.get("year")
    dep_id = request.args.get("dep_id")
    list_production = graduate_programSQL.production_general_db(
        graduate_program_id, year, dep_id
    )
    return jsonify(list_production), 200


@graduateProgramRest.route("/graduate_program", methods=["GET"])
def graduate_program():
    list_gradute_program = []
    institution_id = request.args.get("institution_id")
    if institution_id is None:
        institution_id = ""
    df_bd = graduate_programSQL.graduate_program_db(institution_id)
    for i, infos in df_bd.iterrows():
        graduateProgram = GraduateProgram()
        graduateProgram.graduate_program_id = str(infos.graduate_program_id)
        graduateProgram.code = str(infos.code)
        graduateProgram.name = str(infos.program)
        graduateProgram.area = str(infos.area.strip())
        graduateProgram.modality = str(infos.modality)
        graduateProgram.type = str(infos.type)
        graduateProgram.rating = str(infos.rating)

        list_gradute_program.append(graduateProgram.getJson())

    return jsonify(list_gradute_program), 200


@graduateProgramRest.route("/graduate_program_profnit", methods=["GET"])
def graduate_program_profnit():
    graduate_program_id = request.args.get("id")
    json_graduate_program = graduate_programSQL.graduate_program_profnit_db(
        graduate_program_id
    )
    return jsonify(json_graduate_program), 200


@graduateProgramRest.route("/graduate_program/<id>/graphic/", methods=["GET"])
def basic_graphic(id):
    data = graduate_programSQL
    return jsonify(data), 200


@graduateProgramRest.route("/graduate_program/<id>/graphic/indprod", methods=["GET"])
def indProd_graphic(id):
    data = graduate_programSQL.basic_graphic(id)
    return jsonify(data), 200


@graduateProgramRest.route(
    "/graduate_program/<id>/graphic/indprodsup/",
    methods=["GET"],
)
def IndProdExtSup_graphic(id):
    data = graduate_programSQL
    return jsonify(data), 200
