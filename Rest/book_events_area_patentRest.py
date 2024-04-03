import unidecode
from flask import jsonify, request, Blueprint
from Model.Researcher import Researcher
import Dao.areaFlowSQL as areaFlowSQL
from flask_cors import cross_origin

areaRest = Blueprint("areaRest", __name__)


@areaRest.route("/researcherEvent", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherEvent():
    list_researcher_area_expertise = []

    term = request.args.get("term")
    if term == "":
        return

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    university = ""
    university = str(request.args.get("university")) + ""

    df_bd = areaFlowSQL.lista_researcher_event_db(
        term.lower(), university, graduate_program_id
    )
    for i, infos in df_bd.iterrows():

        r = Researcher()
        r.id = str(infos.id)
        r.name = str(infos.researcher_name)

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

        list_researcher_area_expertise.append(r.getJson())
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route("/researcherPatent", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherPatent():
    list_researcher_area_expertise = []

    term = request.args.get("term")
    if term == "":
        return

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    university = ""
    university = str(request.args.get("university")) + ""

    df_bd = areaFlowSQL.lista_researcher_patent_db(
        term.lower(), university, graduate_program_id
    )
    for i, infos in df_bd.iterrows():

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

        list_researcher_area_expertise.append(r.getJson())
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route("/researcherParticipationEvent", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherParticipationEvent():
    list_researcher = []

    term = request.args.get("term")
    if term == "":
        return

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    # termNovo=unidecode.unidecode(name.replace(";","&"))

    university = ""
    university = str(request.args.get("university")) + ""

    # print(termNovo)
    # print(stemmer.stem(termNovo))
    # df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd = areaFlowSQL.lista_researcher_participation_event_db(
        term.lower(), university, graduate_program_id
    )
    # df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i, infos in df_bd.iterrows():
        # area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        # area_=" "

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

        # print(researcher)
        list_researcher.append(r.getJson())
    return jsonify(list_researcher), 200


@areaRest.route("/researcherBook", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherBook():
    list_researcher = []

    term = request.args.get("term")
    if term == "":
        return
    # stemmer = nltk.RSLPStemmer()

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    # termNovo=unidecode.unidecode(name.replace(";","&"))

    university = ""
    university = str(request.args.get("university")) + ""

    type = str(request.args.get("type")) + ""

    # print(termNovo)
    # print(stemmer.stem(termNovo))
    # df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd = areaFlowSQL.lista_researcher_book_db(
        term.lower(), university, graduate_program_id, type
    )
    # df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i, infos in df_bd.iterrows():
        # area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        # area_=" "

        r = Researcher()
        r.id = str(infos.id)
        r.among = str(infos.qtd)
        r.name = str(infos.researcher_name)

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

        # print(researcher)
        list_researcher.append(r.getJson())
    return jsonify(list_researcher), 200


# Fluxo Area


# print(list_originals_words_initials_term_db("rob"))
@areaRest.route("/area_expertiseInitials", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def area_expertiseInitials():
    list_area_expertise = []
    initials = request.args.get("initials")

    df_bd = areaFlowSQL.lists_great_area_expertise_term_initials_db(
        initials.lower())

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
@cross_origin(origin="*", headers=["Content-Type"])
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
@cross_origin(origin="*", headers=["Content-Type"])
def researcherArea_expertise():
    list_researcher_area_expertise = []
    area = request.args.get("area")
    # stemmer = nltk.RSLPStemmer()

    # termNovo=unidecode.unidecode(name.replace(";","&"))

    university = ""
    university = str(request.args.get("university")) + ""

    # print(termNovo)
    # print(stemmer.stem(termNovo))
    # df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd = areaFlowSQL.lista_researcher_area_expertise_db(
        area.lower(), university)
    # df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i, infos in df_bd.iterrows():
        # area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        # area_=" "

        r = Researcher()
        r.id = str(infos.id)
        r.name = str(infos.researcher_name)

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
        r.lattes_update = str(infos.last_update)

        """
        researcher  = {
        'id': str(infos.id),
        'name': str(infos.researcher_name),
        'articles':str(infos.articles),
        'book_chapters':str(infos.book_chapters),
        'book':str(infos.book),
        'university':str(infos.institution),
        'lattes_id':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id),
        'area':str(infos.area_.replace("_"," ")),
        'abstract':str(infos.abstract),   
        'city':str(infos.city),
        'orcid':str(infos.orcid),
        'image':str(infos.image)

        }
        #print(researcher)
        """
        list_researcher_area_expertise.append(r.getJason())
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route("/researcherArea_specialty", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherArea_specialty():
    list_researcher_area_expertise = []

    area = request.args.get("area_specialty")
    if area == "":
        return
    # stemmer = nltk.RSLPStemmer()

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    # termNovo=unidecode.unidecode(name.replace(";","&"))

    university = ""
    university = str(request.args.get("university")) + ""

    # print(termNovo)
    # print(stemmer.stem(termNovo))
    # df_bd =SimccBD.lista_researcher_name_db(name.lower())
    df_bd = areaFlowSQL.lista_researcher_area_speciality_db(
        area.lower(), university, graduate_program_id
    )
    # df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i, infos in df_bd.iterrows():
        # area_ = areaFlowSQL.lists_great_area_expertise_researcher_db(infos.id)
        # area_=" "
        r = Researcher()
        r.id = str(infos.id)
        r.name = str(infos.researcher_name)

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
        r.lattes_update = str(infos.last_update)
        """
        researcher  = {
        'id': str(infos.id),
        'name': str(infos.researcher_name),
        'articles':str(infos.articles),
        'book_chapters':str(infos.book_chapters),
        'book':str(infos.book),
        'university':str(infos.institution),
        'lattes_id':str(infos.lattes),
        'lattes_10_id':str(infos.lattes_10_id),
        'area':str(infos.area.replace("_"," ")),
        'abstract':str(infos.abstract),   
        'city':str(infos.city),
        'orcid':str(infos.orcid),
        'image':str(infos.image),
        'area_specialty':str(infos.area_specialty)

        }
        #print(researcher)
        """
        list_researcher_area_expertise.append(r.getJson())
    return jsonify(list_researcher_area_expertise), 200


@areaRest.route("/bibliographic_production_article_area", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
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
        list_bibliographic_production_article.append(
            bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


@areaRest.route("/institutionArea", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def institutionArea():
    list_institutionFrequenci = []
    great_area = request.args.get("great_area")
    area_specialty = request.args.get("area_specialty")

    # stemmer = nltk.RSLPStemmer()

    great_area = unidecode.unidecode(great_area.lower())
    area_specialty = unidecode.unidecode(area_specialty.lower())

    university = ""
    university = str(request.args.get("university")) + ""
    # terms = unidecode(terms.lower())
    # print(termNovo)
    # print(stemmer.stem(termNovo))
    df_bd = areaFlowSQL.lista_institution_area_expertise_db(
        great_area, area_specialty, university
    )
    # df_bd.sort_values(by="articles", ascending=False, inplace=True)
    for i, infos in df_bd.iterrows():
        institution = {
            "id": str(infos.id),
            "institution": str(infos.institution),
            "among": str(infos.qtd),
            "image": str(infos.image),
        }
        # print(researcher)
        list_institutionFrequenci.append(institution)

    return jsonify(list_institutionFrequenci), 200


##############################################################################

if __name__ == "__main__":
    # run app in debug mode on port 5000
    areaRest.run(debug=True, port=5001, host="0.0.0.0")
