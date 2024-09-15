import unidecode
from flask import jsonify, request, Blueprint
from Model.Researcher import Researcher
import Dao.areaFlowSQL as areaFlowSQL
from flask_cors import cross_origin
from http import HTTPStatus


areaRest = Blueprint("areaRest", __name__)


@areaRest.route("/researcher_research_project", methods=["GET"])
def researcher_research_project():
    term = request.args.get("term")
    year = request.args.get("year")
    if not year:
        year = 1990
    graduate_program_id = request.args.get("graduate_program_id")
    researcher_id = request.args.get("researcher_id")

    data_frame = areaFlowSQL.researcher_research_project(
        term, year, graduate_program_id, researcher_id
    )
    return jsonify(data_frame), HTTPStatus.OK


@areaRest.route("/researcherEvent", methods=["GET"])
def researcherEvent():
    term = request.args.get("term")
    if not term:
        return jsonify([]), HTTPStatus.BAD_REQUEST

    graduate_program_id = request.args.get("graduate_program_id")
    university = request.args.get("university")

    list_researcher_area_expertise = areaFlowSQL.lista_researcher_event_db(
        term, university, graduate_program_id
    )

    return jsonify(list_researcher_area_expertise), HTTPStatus.OK


@areaRest.route("/researcherPatent", methods=["GET"])
def researcherPatent():
    term = request.args.get("term")
    if not term:
        return jsonify([]), HTTPStatus.OK

    graduate_program_id = request.args.get("graduate_program_id")
    university = request.args.get("university")
    print("alow")
    list_researcher_area_expertise = areaFlowSQL.lista_researcher_patent_db(
        term, university, graduate_program_id
    )

    return jsonify(list_researcher_area_expertise), HTTPStatus.OK


@areaRest.route("/researcherParticipationEvent", methods=["GET"])
def researcherParticipationEvent():
    term = request.args.get("term")
    if not term:
        return jsonify([]), HTTPStatus.BAD_REQUEST
    graduate_program_id = request.args.get("graduate_program_id")
    university = request.args.get("university")

    list_researcher = areaFlowSQL.lista_researcher_participation_event_db(
        term, university, graduate_program_id
    )
    return jsonify(list_researcher), HTTPStatus.OK


@areaRest.route("/researcherBook", methods=["GET"])
def researcherBook():
    term = request.args.get("term")
    if not term:
        return jsonify([]), HTTPStatus.BAD_REQUEST

    graduate_program_id = request.args.get("graduate_program_id")
    university = request.args.get("university")
    book_type = request.args.get("type")

    list_researcher = areaFlowSQL.lista_researcher_book_db(
        term, university, graduate_program_id, book_type
    )

    return jsonify(list_researcher), HTTPStatus.OK


@areaRest.route("/area_expertiseInitials", methods=["GET"])
def area_expertiseInitials():
    list_area_expertise = []
    initials = request.args.get("initials")

    df_bd = areaFlowSQL.lists_great_area_expertise_term_initials_db(initials.lower())

    for i, infos in df_bd.iterrows():
        print(infos.nome)
        researcher = {
            "id": str(infos.id),
            "name": infos.nome.replace("_", " "),
        }
        # print(researcher)
        list_area_expertise.append(researcher)

    return jsonify(list_area_expertise), 200


@areaRest.route("/area_specialitInitials", methods=["GET"])
def area_specialitInitials():
    list_area_specialit = []
    initials = request.args.get("initials")
    area = request.args.get("area")

    graduate_program_id = request.args.get("graduate_program_id")
    # print("yyyyy "+graduate_program_id  )
    if graduate_program_id is None:
        graduate_program_id = ""

    df_bd = areaFlowSQL.lists_area_speciality_term_initials_db(
        initials, area, graduate_program_id
    )

    for i, infos in df_bd.iterrows():
        area_specialit_ = {
            # 'id': str(infos.id),
            #  'great_area':str(infos.great_area.replace("_"," ")) ,
            "area_expertise": str(infos.area_expertise),
            # 'sub_area_expertise':str(infos.sub_area_expertise),
            "area_specialty": str(infos.area_specialty),
        }
        # print(researcher)
        list_area_specialit.append(area_specialit_)

    return jsonify(list_area_specialit), 200


@areaRest.route("/researcherArea_expertise", methods=["GET"])
def researcherArea_expertise():
    area = request.args.get("area")
    if not area:
        return jsonify([]), HTTPStatus.BAD_REQUEST
    university = request.args.get("university")

    list_researcher_area_expertise = areaFlowSQL.lista_researcher_area_expertise_db(
        area, university
    )

    return jsonify(list_researcher_area_expertise), 200


@areaRest.route("/researcherArea_specialty", methods=["GET"])
def researcherArea_specialty():
    area = request.args.get("area_specialty")
    if not area:
        return jsonify([]), HTTPStatus.BAD_REQUEST

    graduate_program_id = request.args.get("graduate_program_id")
    university = request.args.get("university")

    list_researcher_area_expertise = areaFlowSQL.lista_researcher_area_speciality_db(
        area, university, graduate_program_id
    )

    return jsonify(list_researcher_area_expertise), HTTPStatus.OK


@areaRest.route("/bibliographic_production_article_area", methods=["GET"])
def bibliographic_production_article_area():
    list_bibliographic_production_article = []

    great_area = request.args.get("great_area")
    area_specialty = request.args.get("area_specialty")
    year = request.args.get("year")
    qualis = request.args.get("qualis")

    great_area = unidecode.unidecode(great_area.lower())
    area_specialty = unidecode.unidecode(area_specialty.lower())

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    df_bd = areaFlowSQL.lista_production_article_area_expertise_db(
        great_area, area_specialty, year, qualis, graduate_program_id
    )

    for i, infos in df_bd.iterrows():
        bibliographic_production_article_ = {
            "id": str(infos.id),
            "article_institution": infos.article_institution,
            "issn": infos.issn,
            "authors_institution": infos.authors_institution,
            "abstract": infos.abstract,
            "authors": infos.authors,
            "language": infos.language,
            "citations_count": infos.citations_count,
            "pdf": infos.pdf,
            "landing_page_url": infos.landing_page_url,
            "keywords": infos.keywords,
            "title": str(infos.title),
            "researcher": str(infos.researcher),
            "lattes_id": str(infos.lattes_id),
            "lattes_10_id": str(infos.lattes_10_id),
            "area:": str(infos.area),
            "year": str(infos.year),
            "doi": str(infos.doi),
            "qualis": str(infos.qualis),
            "magazine": str(infos.magazine),
            "jif": str(infos.jcr),
            "jcr_link": str(infos.jcr_link),
        }
        list_bibliographic_production_article.append(bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


@areaRest.route("/institutionArea", methods=["GET"])
def institutionArea():
    list_institutionFrequenci = []
    great_area = request.args.get("great_area")
    area_specialty = request.args.get("area_specialty")

    great_area = unidecode.unidecode(great_area.lower())
    area_specialty = unidecode.unidecode(area_specialty.lower())

    university = ""
    university = str(request.args.get("university")) + ""
    df_bd = areaFlowSQL.lista_institution_area_expertise_db(
        great_area, area_specialty, university
    )
    for i, infos in df_bd.iterrows():
        institution = {
            "id": str(infos.id),
            "institution": str(infos.institution),
            "among": str(infos.qtd),
            "image": str(infos.image),
        }
        list_institutionFrequenci.append(institution)

    return jsonify(list_institutionFrequenci), 200


if __name__ == "__main__":
    areaRest.run(debug=True, port=5001, host="0.0.0.0")
