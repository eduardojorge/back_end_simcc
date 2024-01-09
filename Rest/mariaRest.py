from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

import maria

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/Maria", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def Question():
    configMaria = request.get_json()
    responseMaria = maria.text_generator(
        model=configMaria["model"], messages=configMaria["messages"]
    )

    return jsonify(responseMaria), 200
