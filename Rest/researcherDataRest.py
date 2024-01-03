from imageResearcher import download_image
from flask import Blueprint, send_file, request
from flask_cors import cross_origin

researcherDataRest = Blueprint("researcherDataRest", __name__)


@researcherDataRest.route("/ResearcherData/Image", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def Image():
    researcher_id = request.args.get("researcher_id")
    try:
        print()
        path_image = "files/image_researcher/{id}.jpg".format(id=researcher_id)
        return send_file(path_or_file=path_image)
    except:
        download_image(researcher_id)
        path_image = "files/image_researcher/{id}.jpg".format(id=researcher_id)
        return send_file(path_or_file=path_image)
