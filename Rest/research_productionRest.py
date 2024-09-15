from http import HTTPStatus
from flask import Blueprint, jsonify, request

from Dao import productionSQL

productionRest = Blueprint("research_production", __name__)


@productionRest.route("/researcher_production/events", methods=["GET"])
def researcher_events():
    researcher_id = request.args.get("researcher_id")
    year = request.args.get("year")
    participation_events = productionSQL.participation_events(year, researcher_id)
    return jsonify(participation_events), HTTPStatus.OK


@productionRest.route("/researcher_production/papers_magazine", methods=["GET"])
def researcher_papers_magazine():
    researcher_id = request.args.get("researcher_id")
    year = request.args.get("year")
    participation_events = productionSQL.text_in_newpaper_magazine(year, researcher_id)
    return jsonify(participation_events), HTTPStatus.OK
