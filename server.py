import sys

import nltk
import unidecode
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from nltk.tokenize import RegexpTokenizer

import Dao.areaFlowSQL
import Dao.generalSQL
import project
import SimccBD as SimccBD
from Model.Magazine import Magazine
from Model.Researcher import Researcher
from Rest.book_events_area_patentRest import areaRest
from Rest.graduateProgramRest import graduateProgramRest
from Rest.mariaRest import mariaRest
from Rest.researcherDataRest import researcherDataRest
from Rest.researcherTermRest import researcherTermRest

try:
    project.project_env = sys.argv[1]
except Exception as error:
    project.project_env = "4"
try:
    port = sys.argv[2]
except Exception as error:
    port = 8080


def create_app():
    app = Flask(__name__)
    return app


app = create_app()

app.register_blueprint(areaRest)
app.register_blueprint(researcherTermRest)
app.register_blueprint(graduateProgramRest)
app.register_blueprint(researcherDataRest)
app.register_blueprint(mariaRest)

app.config["CORS_HEADERS"] = "Content-Type"

app.route("/")
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route("/secondWord", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def secondWord():
    if not (term := unidecode.unidecode(request.args.get("term").lower())):
        return jsonify("No Content"), 204

    df_bd = Dao.generalSQL.listSecondWord_bd(term)

    stopwords_portuguese = nltk.corpus.stopwords.words("portuguese")
    stopwords_portuguese.append("atraves")
    stopwords_portuguese.append("desde")
    stopwords_english = nltk.corpus.stopwords.words("english")
    tokens = []
    text = ""

    for i, infos in df_bd.iterrows():
        if not (
            (infos.word.lower() in stopwords_portuguese)
            or (infos.word.lower() in stopwords_english)
        ):
            text = text + " " + infos.word

    tokenize = RegexpTokenizer(r"\w+")
    tokens = tokenize.tokenize(text)

    freq = nltk.FreqDist(tokens)
    list_second_word = []
    for word in freq.most_common(10):
        secondWord = {"word": word[0], "freq": word[1]}
        list_second_word.append(secondWord)
    return jsonify(list_second_word), 200


@app.route("/magazine", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def patent_production_researcher():
    list_magazine = []
    magazine_initialis = request.args.get("initials")
    issn = request.args.get("issn")

    df_bd = Dao.generalSQL.lists_magazine_db(magazine_initialis, issn)

    for i, infos in df_bd.iterrows():
        m = Magazine()
        m.id = str(infos.id)
        m.magazine = str(infos.magazine)
        m.issn = str(infos.issn)
        m.jif = str(infos.jcr)
        m.jcr_link = str(infos.jcr_link)
        m.qualis = str(infos.qualis)

        list_magazine.append(m.getJson())

    return jsonify(list_magazine), 200


@app.route("/reasercherInitials", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def reasercherInitials():
    list_researcher = []
    initials = request.args.get("initials")

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    df_bd = SimccBD.lists_researcher_initials_term_db(
        initials.lower(), graduate_program_id
    )

    for i, infos in df_bd.iterrows():
        researcher = {
            "id": str(infos.id),
            "name": infos.nome,
        }
        list_researcher.append(researcher)

    return jsonify(list_researcher), 200


@app.route("/researcher_image", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcher_image():
    df_bd = SimccBD.lista_researcher_full_name_db()
    list_iamge = []

    for i, infos in df_bd.iterrows():
        researcher_image = {
            "id": str(infos.id),
            "lattes": str(infos.lattes),
            "lattes_10_id": str(infos.lattes_10_id),
        }
        list_iamge.append(researcher_image)

    return jsonify(list_iamge), 200


@app.route("/researcherName", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def researcherName():
    list_researcher = []
    name = request.args.get("name")
    if name == "null":
        return jsonify(list_researcher), 200

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    df_bd = SimccBD.lista_researcher_full_name_db_(name.lower(), graduate_program_id)
    for i, infos in df_bd.iterrows():

        r = Researcher()
        r.id = str(infos.id)
        r.name = str(infos.researcher_name)
        r.among = str("0")
        r.articles = str(infos.articles)
        r.book_chapters = str(infos.book_chapters)
        r.book = str(infos.book)
        r.university = str(infos.institution)
        r.lattes_id = str(infos.lattes)
        r.lattes_10_id = str(infos.lattes_10_id)
        r.abstract = str(infos.abstract)
        r.area = str(infos.area.replace("_", " "))
        r.city = str(infos.city)
        r.orcid = str(infos.orcid)
        r.patent = str(infos.patent)
        r.image_university = str(infos.image)
        r.software = str(infos.software)
        r.brand = str(infos.brand)
        r.university = str(infos.institution)
        r.graduation = str(infos.graduation)
        r.lattes_update = str(infos.lattes_update)

        researcher = r.getJson()

        list_researcher.append(researcher)
    return jsonify(list_researcher), 200


@app.route("/total", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def total():
    researcher_total = SimccBD.researcher_total_db()
    institution_total = SimccBD.institution_total_db()
    bibliographic_production_total = SimccBD.bibliographic_production_total_db()

    total_ = [
        {
            "researcher": str(researcher_total),
            "publications": str(bibliographic_production_total),
            "organizations": str(institution_total),
            "version": "1.0.5 (beta)",
        }
    ]

    return jsonify(total_), 200


@app.route("/recently_updated", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def recently_updated():
    list_bibliographic_production_article = list()
    year = request.args.get("year")
    university = str(request.args.get("university"))

    data_frame = SimccBD.recently_updated(year, university)

    for Index, Data in data_frame.iterrows():
        bibliographic_production_article_ = {
            "researcher_id": str(Data.researcher_id),
            "title": str(Data.title),
            "year": str(Data.year),
            "doi": str(Data.doi),
            "qualis": str(Data.qualis),
            "name_periodical": str(Data.magazine),
            "researcher": str(Data.researcher),
            "lattes_id": str(Data.lattes_id),
            "jif": str(Data.jcr),
            "jcr_link": str(Data.jcr_link),
        }
        list_bibliographic_production_article.append(bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


@app.route("/bibliographic_production_article", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def bibliographic_production_article():

    list_bibliographic_production_article = []
    if term := request.args.get("terms"):
        term = unidecode.unidecode(term.lower())
    year = request.args.get("year")
    qualis = request.args.get("qualis")

    university = ""
    university = str(request.args.get("university"))
    distinct = str(request.args.get("distinct"))

    graduate_program_id = request.args.get("graduate_program_id")
    if graduate_program_id is None:
        graduate_program_id = ""

    data_frame = SimccBD.lists_bibliographic_production_article_db(
        term, year, qualis, university, distinct, graduate_program_id
    )

    for i, infos in data_frame.iterrows():
        if distinct == "0":
            bibliographic_production_article_ = {
                "researcher_id": str(infos.researcher_id),
                "title": str(infos.title),
                "year": str(infos.year),
                "doi": str(infos.doi),
                "qualis": str(infos.qualis),
                "name_periodical": str(infos.magazine),
                "researcher": str(infos.researcher),
                "lattes_id": str(infos.lattes_id),
                "jif": str(infos.jcr),
                "jcr_link": str(infos.jcr_link),
                "created_at": str(infos.created_at),
            }

        if distinct == "1":
            bibliographic_production_article_ = {
                "researcher_id": str(infos.researcher_id),
                "title": str(infos.title),
                "year": str(infos.year),
                "doi": str(infos.doi),
                "qualis": str(infos.qualis),
                "name_periodical": str(infos.magazine),
                "jif": str(infos.jcr),
                "jcr_link": str(infos.jcr_link),
            }

        list_bibliographic_production_article.append(bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


if __name__ == "__main__":
    app.run(
        debug=True,
        port=port,
        host="0.0.0.0",
    )
