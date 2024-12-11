import sys
import nltk
import unidecode
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from nltk.tokenize import RegexpTokenizer
from http import HTTPStatus
import Dao.areaFlowSQL
import Dao.generalSQL
import Dao.researcherSQL
import Dao.termFlowSQL
import SimccBD as SimccBD
from Model.Magazine import Magazine
from Rest.book_events_area_patentRest import areaRest
from Rest.graduateProgramRest import graduateProgramRest
from Rest.researcherDataRest import researcherDataRest
from Rest.researcherTermRest import researcherTermRest
from Rest.relevantRest import management
from Rest.mariaRest import mariaRest
from Rest.ufmgRest import ufmgRest
from Rest.research_productionRest import productionRest
from zeep import Client
from csv_powerBI import (
    graduate_program_csv_db,
    ufmg_teacher,
    graduate_program_researcher_csv_db,
    production_distinct_novo_csv_db,
    article_distinct_novo_csv_db,
    researcher_production_novo_csv_db,
    graduate_program_ind_prod_csv_db,
    ind_prod_researcher_csv_db,
    production_coauthors_csv_db,
    researcher_production_year_csv_db,
    researcher_production_year_distinct_csv_db,
    researcher_article_qualis_csv_db,
    researcher_production_csv_db,
    article_qualis_csv_distinct_db,
    researcher_csv_db,
    researcher_production_tecnical_year_csv_db,
    institution_csv_db,
    fat_simcc_bibliographic_production,
    dim_researcher_csv_db,
    dim_institution_csv_db,
    fat_production_tecnical_year_novo_csv_db,
    dim_city_csv_db,
    fat_foment,
    dim_category_level_code,
    dim_research_group,
    fat_group_leaders,
    dim_departament_researcher,
    dim_departament_technician,
    graduate_program_researcher_year_unnest,
    dim_graduate_program_acronym,
    dim_graduate_program_student_year_unnest,
    graduate_program_student_researcher_csv_db,
    save_data_to_csv,
    fat_departament_csv_bd,
)

YEAR = 1990
try:
    port = sys.argv[2]
except Exception:
    port = 8080


def create_app():
    app = Flask(__name__)
    app.register_blueprint(ufmgRest)
    app.register_blueprint(areaRest)
    app.register_blueprint(researcherTermRest)
    app.register_blueprint(graduateProgramRest)
    app.register_blueprint(researcherDataRest)
    app.register_blueprint(mariaRest)
    app.register_blueprint(productionRest)
    app.register_blueprint(management)

    CORS(app)
    return app


# client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
app = create_app()


@app.route("/secondWord", methods=["GET"])
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
def researcherName():
    name = request.args.get("name")
    graduate_program_id = request.args.get("graduate_program_id")
    dep_id = request.args.get("dep_id")
    list_researcher = Dao.areaFlowSQL.lista_researcher_full_name_db(
        name, graduate_program_id, dep_id
    )
    return jsonify(list_researcher), HTTPStatus.OK


@app.route("/total", methods=["GET"])
def total():
    researcher_total = SimccBD.researcher_total_db()
    institution_total = SimccBD.institution_total_db()
    bibliographic_production_total = SimccBD.bibliographic_production_total_db()

    total = {
        "researcher": str(researcher_total),
        "publications": str(bibliographic_production_total),
        "organizations": str(institution_total),
        "version": "3.0.0 (beta)",
    }

    return total


@app.route("/recently_updated", methods=["GET"])
def recently_updated():
    list_bibliographic_production_article = list()
    year = request.args.get("year")
    if not year:
        year = YEAR
    university = request.args.get("university")
    departament = request.args.get("dep_id")
    data_frame = SimccBD.recently_updated_db(year, university, departament)

    for Index, infos in data_frame.iterrows():
        bibliographic_production_article_ = {
            "researcher_id": str(infos.researcher_id),
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
            "year": str(infos.year),
            "doi": str(infos.doi),
            "qualis": str(infos.qualis),
            "name_periodical": str(infos.magazine),
            "researcher": str(infos.researcher),
            "lattes_id": str(infos.lattes_id),
            "jif": str(infos.jcr),
            "jcr_link": str(infos.jcr_link),
        }
        list_bibliographic_production_article.append(bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


@app.route("/bibliographic_production_article", methods=["GET"])
def bibliographic_production_article():
    list_bibliographic_production_article = []

    terms = request.args.get("terms")
    year = request.args.get("year")
    if not year:
        year = YEAR
    qualis = request.args.get("qualis")
    university = request.args.get("university")
    distinct = request.args.get("distinct")
    graduate_program_id = request.args.get("graduate_program_id")
    dep_id = request.args.get("dep_id")
    page = request.args.get("page", 0, int)

    df_bd = SimccBD.lists_bibliographic_production_article_db(
        terms, year, qualis, university, distinct, graduate_program_id, dep_id, page
    )

    for i, infos in df_bd.iterrows():
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
            }

        if distinct == "1":
            bibliographic_production_article_ = {
                "title": str(infos.title),
                "year": str(infos.year),
                "doi": str(infos.doi),
                "qualis": str(infos.qualis),
                "name_periodical": str(infos.magazine),
                "jif": str(infos.jcr),
                "jcr_link": str(infos.jcr_link),
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
            }

        list_bibliographic_production_article.append(bibliographic_production_article_)

    return jsonify(list_bibliographic_production_article), 200


@app.route("/getCurriculoCompactado", methods=["GET"])
def getCurriculoCompactado():
    client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
    lattes_id = request.args.get("lattes_id")
    try:
        resultado = client.service.getCurriculoCompactado(lattes_id)
        arquivo = open(f"Files/indicadores_simcc/xmls/{lattes_id}.zip", "wb")
        arquivo.write(resultado)
        arquivo.close()
        return send_file(
            f"Files/indicadores_simcc/xmls/{lattes_id}.zip", mimetype="application/zip"
        )
    except Exception:
        return jsonify("Curriculo não encontrado")


@app.route("/getDataAtualizacaoCV", methods=["GET"])
def getDataAtualizacaoCV():
    client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
    lattes_id = request.args.get("lattes_id")
    resultado = client.service.getDataAtualizacaoCV(lattes_id)
    return jsonify(resultado)


@app.route("/research_group", methods=["GET"])
def get_research_group():
    group_id = request.args.get("group_id")
    research_group = SimccBD.lists_research_groups(group_id)
    return research_group


@app.route("/research_group_lines", methods=["GET"])
def list_research_lines():
    group_id = request.args.get("group_id")
    research_group = SimccBD.list_research_lines(group_id)
    return research_group


@app.route("/research_group/count", methods=["GET"])
def get_research_group_count():
    research_group = SimccBD.list_count_researcher_groups()
    return research_group


@app.route("/productivity_research", methods=["GET"])
def get_productivityResearch():
    productivity_research = SimccBD.list_productivity_research()
    return productivity_research


@app.route("/cimatec_graduate_program.csv", methods=["GET"])
def load_graduate_program_csv_db():
    graduate_program_csv_db()
    path = "Files/indicadores_simcc/cimatec_graduate_program.csv"
    return send_file(path, as_attachment=True)


@app.route("/cimatec_graduate_program_researcher.csv", methods=["GET"])
def load_graduate_program_researcher_csv_db():
    graduate_program_researcher_csv_db()
    path = "Files/indicadores_simcc/cimatec_graduate_program_researcher.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_distinct_novo_csv_db.csv", methods=["GET"])
def load_production_distinct_novo_csv_db():
    production_distinct_novo_csv_db()
    path = "Files/indicadores_simcc/production_distinct_novo_csv_db.csv"
    return send_file(path, as_attachment=True)


@app.route("/article_distinct_novo_csv_db.csv", methods=["GET"])
def load_article_distinct_novo_csv_db():
    article_distinct_novo_csv_db()
    path = "Files/indicadores_simcc/article_distinct_novo_csv_db.csv"
    return send_file(path, as_attachment=True)


@app.route("/researcher_production_novo_csv_db.csv", methods=["GET"])
def load_researcher_production_novo_csv_db():
    researcher_production_novo_csv_db()
    path = "Files/indicadores_simcc/researcher_production_novo_csv_db.csv"
    return send_file(path, as_attachment=True)


@app.route("/graduate_program_ind_prod.csv", methods=["GET"])
def load_graduate_program_ind_prod_csv_db():
    graduate_program_ind_prod_csv_db()
    path = "Files/indicadores_simcc/graduate_program_ind_prod.csv"
    return send_file(path, as_attachment=True)


@app.route("/fat_researcher_ind_prod.csv", methods=["GET"])
def load_ind_prod_researcher_csv_db():
    ind_prod_researcher_csv_db()
    path = "Files/indicadores_simcc/fat_researcher_ind_prod.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_coauthors_csv_db.csv", methods=["GET"])
def load_production_coauthors_csv_db():
    production_coauthors_csv_db()
    path = "Files/indicadores_simcc/production_coauthors_csv_db.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_year.csv", methods=["GET"])
def load_researcher_production_year_csv_db():
    researcher_production_year_csv_db()
    path = "Files/indicadores_simcc/production_year.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_year_distinct.csv", methods=["GET"])
def load_researcher_production_year_distinct_csv_db():
    researcher_production_year_distinct_csv_db()
    path = "Files/indicadores_simcc/production_year_distinct.csv"
    return send_file(path, as_attachment=True)


@app.route("/article_qualis_year.csv", methods=["GET"])
def load_researcher_article_qualis_csv_db():
    researcher_article_qualis_csv_db()
    path = "Files/indicadores_simcc/article_qualis_year.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_researcher.csv", methods=["GET"])
def load_researcher_production_csv_db():
    researcher_production_csv_db()
    path = "Files/indicadores_simcc/production_researcher.csv"
    return send_file(path, as_attachment=True)


@app.route("/article_qualis_year_institution.csv", methods=["GET"])
def load_article_qualis_csv_distinct_db():
    article_qualis_csv_distinct_db()
    path = "Files/indicadores_simcc/article_qualis_year_institution.csv"
    return send_file(path, as_attachment=True)


@app.route("/researcher.csv", methods=["GET"])
def load_researcher_csv_db():
    researcher_csv_db()
    path = "Files/indicadores_simcc/researcher.csv"
    return send_file(path, as_attachment=True)


@app.route("/production_tecnical_year.csv", methods=["GET"])
def load_researcher_production_tecnical_year_csv_db():
    researcher_production_tecnical_year_csv_db()
    path = "Files/indicadores_simcc/production_tecnical_year.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_institution.csv", methods=["GET"])
def load_institution_csv_db():
    institution_csv_db()
    path = "Files/indicadores_simcc/dim_institution.csv"
    return send_file(path, as_attachment=True)


@app.route("/fat_simcc_bibliographic_production.csv", methods=["GET"])
def load_fat_simcc_bibliographic_production():
    fat_simcc_bibliographic_production()
    path = "Files/indicadores_simcc/fat_simcc_bibliographic_production.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_researcher.csv", methods=["GET"])
def load_dim_researcher_csv_db():
    dim_researcher_csv_db()
    path = "Files/indicadores_simcc/dim_researcher.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_institution.csv", methods=["GET"])
def load_dim_institution_csv_db():
    dim_institution_csv_db()
    path = "Files/indicadores_simcc/dim_institution.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_institution.csv", methods=["GET"])
def load_dim_city_csv_db():
    dim_city_csv_db()
    path = "Files/indicadores_simcc/dim_institution.csv"
    return send_file(path, as_attachment=True)


@app.route("/fat_production_tecnical_year_novo_csv_db.csv", methods=["GET"])
def load_fat_production_tecnical_year_novo_csv_db():
    fat_production_tecnical_year_novo_csv_db()
    path = "Files/indicadores_simcc/fat_production_tecnical_year_novo_csv_db.csv"
    return send_file(path, as_attachment=True)


@app.route("/fat_foment.csv", methods=["GET"])
def load_fat_foment():
    fat_foment()
    path = "Files/indicadores_simcc/fat_foment.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_category_level_code.csv", methods=["GET"])
def load_dim_category_level_code():
    dim_category_level_code()
    path = "Files/indicadores_simcc/dim_category_level_code.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_research_group.csv", methods=["GET"])
def load_dim_research_group():
    dim_research_group()
    path = "Files/indicadores_simcc/dim_research_group.csv"
    return send_file(path, as_attachment=True)


@app.route("/fat_group_leaders.csv", methods=["GET"])
def load_fat_group_leaders():
    fat_group_leaders()
    path = "Files/indicadores_simcc/fat_group_leaders.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_departament_researcher.csv", methods=["GET"])
def load_dim_departament_researcher():
    dim_departament_researcher()
    path = "Files/indicadores_simcc/dim_departament_researcher.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_departament_technician.csv", methods=["GET"])
def load_dim_departament_technician():
    dim_departament_technician()
    path = "Files/indicadores_simcc/dim_departament_technician.csv"
    return send_file(path, as_attachment=True)


@app.route("/graduate_program_researcher_year_unnest.csv", methods=["GET"])
def load_graduate_program_researcher_year_unnest():
    graduate_program_researcher_year_unnest()
    path = "Files/indicadores_simcc/graduate_program_researcher_year_unnest.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_graduate_program_acronym.csv", methods=["GET"])
def load_dim_graduate_program_acronym():
    dim_graduate_program_acronym()
    path = "Files/indicadores_simcc/dim_graduate_program_acronym.csv"
    return send_file(path, as_attachment=True)


@app.route("/graduate_program_student_year_unnest.csv", methods=["GET"])
def load_dim_graduate_program_student_year_unnest():
    dim_graduate_program_student_year_unnest()
    path = "Files/indicadores_simcc/graduate_program_student_year_unnest.csv"
    return send_file(path, as_attachment=True)


@app.route("/cimatec_graduate_program_student.csv", methods=["GET"])
def load_graduate_program_student_researcher_csv_db():
    graduate_program_student_researcher_csv_db()
    path = "Files/indicadores_simcc/cimatec_graduate_program_student.csv"
    return send_file(path, as_attachment=True)


@app.route("/data.csv", methods=["GET"])
def load_save_data_to_csv():
    save_data_to_csv()
    path = "Files/indicadores_simcc/data.csv"
    return send_file(path, as_attachment=True)


@app.route("/dim_departament.csv", methods=["GET"])
def load_fat_departament_csv_bd():
    fat_departament_csv_bd()
    path = "Files/indicadores_simcc/dim_departament.csv"
    return send_file(path, as_attachment=True)


@app.route("/Qualis.xlsx", methods=["GET"])
def load_Qualis():
    path = "Files/indicadores_simcc/Qualis.xlsx"
    return send_file(path, as_attachment=True)


@app.route("/platform_image.xlsx", methods=["GET"])
def load_platform_image():
    path = "Files/indicadores_simcc/platform_image.xlsx"
    return send_file(path, as_attachment=True)


@app.route("/DimensaoTipoProducao.xlsx", methods=["GET"])
def load_DimensaoTipoProducao():
    path = "Files/indicadores_simcc/DimensaoTipoProducao.xlsx"
    return send_file(path, as_attachment=True)


@app.route("/DimensaoAno.xlsx", methods=["GET"])
def load_DimensaoAnog():
    path = "Files/indicadores_simcc/DimensaoAno.xlsx"
    return send_file(path, as_attachment=True)


@app.route("/ufmg_teacher.csv", methods=["GET"])
def load_ufmg_tearcher():
    ufmg_teacher()
    path = "Files/indicadores_simcc/ufmg_teacher.csv"
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    app.run(
        debug=True,
        port=port,
        host="0.0.0.0",
    )
