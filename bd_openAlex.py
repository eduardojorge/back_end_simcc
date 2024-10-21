from Dao import sgbdSQL
import os
import json
from dotenv import load_dotenv

load_dotenv(override=True)


def extract_institutions(data):
    for author in data["authorships"]:
        for institution in author["institutions"]:
            script_sql = """
            SELECT
                api.id
            FROM
                public.open_alex_institutions api
            WHERE
                similarity(
                    unaccent(LOWER(%s)),
                    unaccent(LOWER(api.name))) > 0.4
            LIMIT 1;
            """

            params = (institution["display_name"].replace("'", "''"),)

            if not sgbdSQL.consultar_db(script_sql, params):
                script_sql_insert = """
                    INSERT INTO public.open_alex_institutions(
                    name, ror, country_code, type, lineage)
                    VALUES (%s, %s, %s, %s, %s);
                """

                insert_params = (
                    institution["display_name"],
                    institution["ror"],
                    institution["country_code"],
                    institution["type"],
                    institution["lineage"][0],
                )

                sgbdSQL.execScript_db(script_sql_insert, insert_params)
            else:
                ...


def extract_article_tmp(id, data):
    try:
        issn = str(", ").join(data["primary_location"]["source"]["issn"])
    except Exception:
        issn = str()

    try:
        article_institution = data["primary_location"]["source"]["display_name"]
    except Exception:
        article_institution = str()

    op_abstract = data["abstract_inverted_index"]

    try:
        length = max(op_abstract.values())[0]
        abstract = list(range(length + 1))
        for item in op_abstract.items():
            for word in item[1]:
                abstract[word] = item[0].replace("'", " ")
        abstract = str(" ").join(abstract)
    except Exception:
        abstract = str()

    authors = data["authorships"]
    authors_list = []
    for author in authors:
        authors_list.append(author["author"]["display_name"].replace("'", " "))
    authors_list = str("; ").join(authors_list)

    institutions_list = []
    for author in authors:
        try:
            institutions_list.append(author["institutions"][0]["display_name"])
        except Exception:
            institutions_list.append("")
    institutions_list = str("; ").join(institutions_list)

    language = data["language"]

    citations_count = sum(count["cited_by_count"] for count in data["counts_by_year"])

    download_link = data["primary_location"]["pdf_url"]
    landing_page_url = data["primary_location"]["landing_page_url"]

    keywords_list = [keyword["display_name"] for keyword in data["keywords"]]
    keywords_list = str("; ").join(keywords_list)

    script_sql = """
        INSERT INTO public.openalex_article(
        article_id, article_institution, issn, abstract, authors,
        authors_institution, language, citations_count, pdf, landing_page_url,
        keywords)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    params = (
        id,
        article_institution,
        issn,
        abstract,
        authors_list,
        institutions_list,
        language,
        citations_count,
        download_link,
        landing_page_url,
        keywords_list,
    )

    sgbdSQL.execScript_db(script_sql, params)


def extract_researcher_tmp(id, data):
    h_index = data["summary_stats"]["h_index"]
    i10_index = data["summary_stats"]["i10_index"]
    orcid = data["ids"]["orcid"][-18:]
    open_alex = data["id"]
    works_count = data["works_count"]
    cited_by_count = data["cited_by_count"]

    script_sql = """
        INSERT INTO public.openalex_researcher(
        researcher_id, h_index, relevance_score, works_count, cited_by_count, i10_index, scopus, orcid, openalex)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    params = (
        id,
        h_index,
        0,
        works_count,
        cited_by_count,
        i10_index,
        "",
        orcid,
        open_alex,
    )

    sgbdSQL.execScript_db(script_sql, params)


if __name__ == "__main__":
    for json_path in os.listdir("Files/openAlex_article"):
        if json_path.endswith(".json"):
            with open(f"Files/openAlex_article/{json_path}", "r") as f:
                data = json.load(f)
                extract_article_tmp(json_path[:-5], data)
    for json_path in os.listdir("Files/openAlex_researcher"):
        if json_path.endswith(".json"):
            with open(f"Files/openAlex_researcher/{json_path}", "r") as f:
                data = json.load(f)
                extract_researcher_tmp(json_path[:-5], data)
