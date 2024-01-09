from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

import maria

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/Maria", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def Maria():
    configMaria = request.get_json()
    responseMaria = maria.text_generator(
        model=configMaria["model"], messages=configMaria["messages"]
    )

    fullJson = list()
    for sliceResponse in responseMaria.split("</s>"):
        chat = sliceResponse.split("|>")

        chat = {"role": f"{chat[0][2:]}", "content": f"{chat[1]}"}
        fullJson.append(chat)
    return jsonify(fullJson), 200
