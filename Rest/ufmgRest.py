from flask import Blueprint, jsonify
from Dao import ufmgSQL

ufmgRest = Blueprint("ufmgRest", __name__)


@ufmgRest.route("/departament/rt", methods=["GET"])
def departament():
    rt_list = ufmgSQL.get_rt_list()
    return jsonify(rt_list)
