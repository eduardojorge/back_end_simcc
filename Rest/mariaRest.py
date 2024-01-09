from flask import Blueprint, jsonify, request
from flask_cors import cross_origin
from maria import Question

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/Maria", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def QuestionMaria():
    mariaSetup = request.get_json()
    return jsonify(Question(messages=mariaSetup["messages"], model=mariaSetup["model"]))
