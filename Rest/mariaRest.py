from http import HTTPStatus
from flask import Blueprint, jsonify, request
import psycopg2
from Dao import mariaSQL

mariaRest = Blueprint("mariaRest", __name__)


@mariaRest.route("/maria/researcher/abstract", methods=["GET"])
def researcher_abstract():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "abstract")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR


@mariaRest.route("/maria/researcher/article", methods=["GET"])
def researcher_article():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "article")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR


@mariaRest.route("/maria/researcher/article_abstract", methods=["GET"])
def researcher_article_abstract():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "article_abstract")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR


@mariaRest.route("/maria/researcher/book", methods=["GET"])
def researcher_book():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "book")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR


@mariaRest.route("/maria/researcher/event", methods=["GET"])
def researcher_event():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "event")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR


@mariaRest.route("/maria/researcher/patent", methods=["GET"])
def researcher_patent():
    try:
        query = request.args.get("query")
        if not query:
            return []
        researcher = mariaSQL.search_by_embeddings(query, "patent")
        researcher = mariaSQL.mount_researchers(researcher)
        comment = mariaSQL.mount_comment(researcher)
        return jsonify({"query": comment, "researcher": researcher}), HTTPStatus.OK

    except psycopg2.errors.UndefinedTable:
        return jsonify({"query": "", "researcher": []}), HTTPStatus.OK

    except Exception as e:
        return jsonify(
            {"error": str(e), "query": "", "researcher": []}
        ), HTTPStatus.INTERNAL_SERVER_ERROR
