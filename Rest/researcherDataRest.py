from imageResearcher import download_image
from flask import Blueprint, send_file, request, jsonify
from flask_cors import cross_origin

import pandas as pd

import Dao.researcherSQL as researcherSQL
import Dao.generalSQL as generalSQL
from Model.City import City
from Model.Researcher import Researcher

researcherDataRest = Blueprint("researcherDataRest", __name__)


@researcherDataRest.route("/ResearcherData/Image", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def image():
    researcher_id = request.args.get("researcher_id")
    try:
        path_image = "files/image_researcher/{id}.jpg".format(id=researcher_id)
        return send_file(path_or_file=path_image)
    except:
        download_image(researcher_id)
        path_image = "files/image_researcher/{id}.jpg".format(id=researcher_id)
        return send_file(path_or_file=path_image)


@researcherDataRest.route("/ResearcherData/ByCity", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def byCity():
    city_id = researcherSQL.city_search(request.args.get("city"))
    researchers = researcherSQL.researcher_search_city(city_id)

    JsonResearchers = list()
    for Index, researcher in researchers.iterrows():
        if city_id == None:
            dict_researcher = {
                "id": researcher["id"],
                "researcher_name": researcher["researcher_name"],
                "institution": researcher["institution"],
                "image": researcher["image"],
                "city": researcher["city"],
            }
            JsonResearchers.append(dict_researcher)
        else:
            researcher_inst = Researcher()

            researcher_inst.id = researcher["id"]
            researcher_inst.name = researcher["researcher_name"]
            researcher_inst.university = researcher["institution"]
            researcher_inst.articles = researcher["articles"]
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


# Trocar essa chamada de lugar assim que possivel


@researcherDataRest.route("/ResearcherData/TaxonomyCSV", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def getCSV():
    csv_taxonomy = pd.read_csv("/home/lilith/repository/back_simcc/article_tax.csv")

    JsonTax = list()
    for Index, Taxonomy in csv_taxonomy.iterrows():
        Tax = {
            "index": Taxonomy["index"],
            "researcher_id": Taxonomy["researcher_id"],
            "title": Taxonomy["title"],
            "institution_id": Taxonomy["institution_id"],
            "city_id": Taxonomy["city_id"],
            "year": Taxonomy["year"],
            "qualis": Taxonomy["qualis"],
            "jcr": Taxonomy["jcr"],
            "magazine_name": Taxonomy["magazine_name"],
            "tax_id": Taxonomy["tax_id"],
        }
        JsonTax.append(Tax)
    return jsonify(JsonTax)


@researcherDataRest.route("/ResearcherData/City", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def city():
    city_data_frame = generalSQL.queryCity()

    JsonCity = list()
    for Index, city_data in city_data_frame.iterrows():
        city = City(
            id=city_data["id"],
            name=city_data["name"],
            country_id=city_data["country_id"],
            state_id=city_data["state_id"],
        )

        JsonCity.append(city.getJson())
    return jsonify(JsonCity)
