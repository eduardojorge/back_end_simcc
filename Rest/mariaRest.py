import rest_maria
from flask import jsonify, request, Blueprint
from flask_cors import cross_origin
from Dao import taxonomySQL
from Model.Researcher import Researcher
from pprint import pprint

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/Maria/Chat", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def chat():
    return


@mariaRest.route("/Maria/CreateTaxonomy", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def CreateTaxonomy():
    question = request.args.get("question")
    term = request.args.get("term")

    taxonomy = rest_maria.create_taxonomy(question=question, term=term)

    return jsonify(taxonomy), 200


@mariaRest.route("/Maria/SearchTaxonomy", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def SearchTaxonomy():
    Json_Taxonomy = request.get_json()

    term_list = rest_maria.read_json(Json_Taxonomy)

    list_researcher = list()

    for term in term_list:
        data_frame = taxonomySQL.search_term(term=term)

        for Index, infos in data_frame.iterrows():
            r = Researcher()
            r.id = str(infos.id)
            r.name = str(infos.researcher_name)
            r.among = str(infos.qtd)
            r.articles = str(infos.articles)
            r.book_chapters = str(infos.book_chapters)
            r.book = str(infos.book)
            r.patent = str(infos.patent)
            r.software = str(infos.software)
            r.brand = str(infos.brand)
            r.university = str(infos.institution)
            r.lattes_id = str(infos.lattes)
            r.lattes_10_id = str(infos.lattes_10_id)
            r.abstract = str(infos.abstract)
            r.area = str(infos.area.replace("_", " "))
            r.city = str(infos.city)
            r.orcid = str(infos.orcid)
            r.image_university = str(infos.image)
            r.graduation = str(infos.graduation)
            r.lattes_update = str(infos.lattes_update)

            researcher = r.getJson()
            researcher["term"] = str(" ").join(term)
            list_researcher.append(researcher)

    return jsonify(list_researcher), 200
