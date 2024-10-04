import pandas as pd
import Dao.sgbdSQL as db
import bd_maria
from langchain_openai import ChatOpenAI

maria = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.9)


def search_by_embeddings(query, search_type):
    embedding = bd_maria.get_embeddings(query)

    script_sql = f"""
        SELECT reference_id, 1 - (embeddings <=> '{embedding}') AS cosine_similarity
        FROM embeddings.{search_type}
        ORDER BY cosine_similarity desc
        LIMIT 10;
        """

    registry = db.consultar_db(script_sql)
    data_frame = pd.DataFrame(registry, columns=["id", "proximidade"])

    data_frame = get_researcher_id(data_frame, search_type)
    return data_frame


def get_researcher_id(data, search_type):
    if search_type in ["article", "book", "event", "article_abstract"]:
        SCRIPT_SQL = f"""
            SELECT researcher_id
            FROM bibliographic_production
            WHERE id IN {tuple(data['id'].to_list())}
            """
        registry = db.consultar_db(SCRIPT_SQL)
        data_frame = pd.DataFrame(registry, columns=["researcher_id"])
        return data_frame
    elif search_type == "abstract":
        data["researcher_id"] = data["id"]
        return data
    elif search_type == "patent":
        SCRIPT_SQL = f"""
            SELECT researcher_id
            FROM patent
            WHERE id IN {tuple(data['id'].to_list())}
            """
        registry = db.consultar_db(SCRIPT_SQL)
        data_frame = pd.DataFrame(registry, columns=["researcher_id"])
        return data_frame


def mount_researchers(data_frame):
    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            COUNT(DISTINCT b.id) as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            RIGHT JOIN bibliographic_production b ON b.researcher_id = r.id
        WHERE
            r.id IN {tuple(data_frame['researcher_id'].to_list())}
        GROUP BY
            r.id, r.name, r.lattes_id, rp.articles, rp.book_chapters,
            rp.book, rp.software, rp.brand, i.name, r.abstract,
            rp.great_area, rp.city, r.orcid, i.image, r.graduation,
            r.last_update, rp.patent
        ORDER BY
            among DESC;
            """
    registry = db.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )
    return data_frame.fillna("").to_dict(orient="records")


def mount_comment(data_dict):
    PROMPT_TEMPLATE = f"""
    Você é um chatbot chamado Maria, especializada em auxiliar pesquisadores. Gostaria que você analisasse os dados de alguns pesquisadores e me fornecesse um resumo conciso da seção de resultados, destacando os principais achados e suas implicações para a área de [área de pesquisa]. Por favor, utilize uma linguagem clara e objetiva, adequada para um público com conhecimento intermediário. Além disso, gostaria que você indicasse quais outras pesquisas poderiam complementar este estudo e se existem lacunas de conhecimento ainda a serem exploradas.

    {data_dict}
    """

    response = maria.invoke(PROMPT_TEMPLATE)
    return str(response.content)
