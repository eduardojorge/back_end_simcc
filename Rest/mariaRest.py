from http import HTTPStatus
from flask import Blueprint, jsonify, request

from Dao import mariaSQL

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/maria/researcher/abstract", methods=["GET"])
def researcher_abstract():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "abstract")
    researcher = mariaSQL.mount_researchers(researcher)
    comment = mariaSQL.mount_comment(researcher)
    return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK


@mariaRest.route("/maria/researcher/article", methods=["GET"])
def researcher_article():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "article")
    researcher = mariaSQL.mount_researchers(researcher)
    return jsonify(researcher), HTTPStatus.OK


@mariaRest.route("/maria/researcher/article_abstract", methods=["GET"])
def researcher_article_abstract():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "article_abstract")
    researcher = mariaSQL.mount_researchers(researcher)
    return jsonify(researcher), HTTPStatus.OK


@mariaRest.route("/maria/researcher/book", methods=["GET"])
def researcher_book():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "book")
    researcher = mariaSQL.mount_researchers(researcher)
    return jsonify(researcher), HTTPStatus.OK


@mariaRest.route("/maria/researcher/event", methods=["GET"])
def researcher_event():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "event")
    researcher = mariaSQL.mount_researchers(researcher)
    return jsonify(researcher), HTTPStatus.OK


@mariaRest.route("/maria/researcher/patent", methods=["GET"])
def researcher_patent():
    query = request.args.get("query")
    researcher = mariaSQL.search_by_embeddings(query, "patent")
    researcher = mariaSQL.mount_researchers(researcher)
    return jsonify(researcher), HTTPStatus.OK
