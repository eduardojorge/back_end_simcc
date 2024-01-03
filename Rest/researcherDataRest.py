from imageResearcher import download_image
from flask import Blueprint, send_file, request, jsonify
from flask_cors import cross_origin

import Dao.researcherSQL as researcherSQL
from Model.Researcher import Researcher

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


@researcherDataRest.route("/ResearcherData/ByCity", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def ByCity():
    city_id = researcherSQL.city_search(request.args.get("city"))
    researchers = researcherSQL.researcher_search_city(city_id)

    JsonResearchers = list()
    for Index, researcher in researchers.iterrows():
        researcher_inst = Researcher()

        researcher_inst.id = researcher["id"]
        researcher_inst.name = researcher["researcher_name"]
        researcher_inst.university = researcher["institution"]
        researcher_inst.articles = researcher["article"]
        researcher_inst.book_chapters = researcher["book_chapters"]
        researcher_inst.book = researcher["book"]
        researcher_inst.lattes_id = researcher["lattes"]
        researcher_inst.lattes_10_id = researcher["lattes_10_id"]
        researcher_inst.abstract = researcher["abstract"]
        researcher_inst.area = researcher["area"].replace("_", " ")
        researcher_inst.city = researcher["city"]
        researcher_inst.image_university = researcher["image"]
        researcher_inst.orcid = researcher["orcid"]
        researcher_inst.graduation = researcher["graduation"]
        researcher_inst.patent = researcher["patent"]
        researcher_inst.software = researcher["software"]
        researcher_inst.brand = researcher["brand"]
        researcher_inst.lattes_update = researcher["lattes_update"]

        JsonResearchers.append(researcher_inst.getJson())
    return jsonify(JsonResearchers), 200
