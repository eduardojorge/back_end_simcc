from flask import jsonify, request, Blueprint
from flask_cors import cross_origin
import Dao.graduate_programSQL as graduate_programSQL
from Model.GraduateProgram import GraduateProgram

graduateProgramRest = Blueprint("graduateProgramRest", __name__)


# print(list_originals_words_initials_term_db("rob"))
@graduateProgramRest.route("/graduate_program_production", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def graduate_program_production():
    graduate_program_id = request.args.get("graduate_program_id")

    year = request.args.get("year")

    return (
        jsonify(graduate_programSQL.production_general_db(graduate_program_id, year)),
        200,
    )


# print(list_originals_words_initials_term_db("rob"))
@graduateProgramRest.route("/graduate_program", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def graduate_program():
    list_gradute_program = []
    institution_id = request.args.get("institution_id")
    # print("yyyyy "+graduate_program_id  )
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


# print(list_originals_words_initials_term_db("rob"))
@graduateProgramRest.route("/graduate_program_profnit", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def graduate_program_profnit():

    list_gradute_program = list()
    graduate_program_id = request.args.get("id")

    df_bd = graduate_programSQL.graduate_program_profnit_db(graduate_program_id)
    for i, infos in df_bd.iterrows():
        graduateProgram = GraduateProgram()
        graduateProgram.graduate_program_id = str(infos.graduate_program_id)
        graduateProgram.code = str(infos.code)
        graduateProgram.name = str(infos.program)
        graduateProgram.area = str(infos.area.strip())
        graduateProgram.modality = str(infos.modality)
        graduateProgram.type = str(infos.type)
        graduateProgram.rating = str(infos.rating)
        graduateProgram.state = str(infos.state)
        graduateProgram.city = str(infos.city)
        graduateProgram.instituicao = str(infos.instituicao)
        graduateProgram.url_image = str(infos.url_image)
        graduateProgram.region = str(infos.region)
        graduateProgram.sigla = infos.sigla

        list_gradute_program.append(graduateProgram.getJson())

    return jsonify(list_gradute_program), 200


# if __name__ == "__main__":
# cimatecRest.run(debug=True, port=5001, host="0.0.0.0")
