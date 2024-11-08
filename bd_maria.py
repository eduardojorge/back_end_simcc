from langchain_openai import OpenAIEmbeddings
from Dao import sgbdSQL
import pandas as pd
from config import settings


def get_embeddings(text):
    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-large", openai_api_key=settings.OPENAI_API_KEY
    )
    embeddings = embeddings_model.embed_query(text)
    return embeddings


def count_tokens(texto):
    return 0


def get_custos(total_tokens):
    return 0


if __name__ == "__main__":
    print("ETAPA 1")
    SCRIPT_SQL = """
        SELECT id, abstract 
        FROM researcher 
        WHERE id NOT IN 
        (SELECT reference_id FROM embeddings.abstract);
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.abstract (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.MARIA_DATABASE_PORT,
            )

    print("ETAPA 2")
    SCRIPT_SQL = """
        SELECT id, title 
        FROM bibliographic_production 
        WHERE 
        type = 'ARTICLE'
        AND id NOT IN 
        (SELECT reference_id FROM embeddings.article)
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.article (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.PORT,
            )

    print("ETAPA 3")
    SCRIPT_SQL = """
        SELECT id, abstract 
        FROM openalex_article 
        WHERE article_id NOT IN 
        (SELECT reference_id FROM embeddings.article_abstract)
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.article_abstract (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.PORT,
            )

    print("ETAPA 4")
    SCRIPT_SQL = """
        SELECT id, title 
        FROM bibliographic_production
        WHERE 
        type = 'BOOK'
        AND id NOT IN
        (SELECT reference_id FROM embeddings.book)
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.book (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.PORT,
            )

    print("ETAPA 5")
    SCRIPT_SQL = """
        SELECT id, title 
        FROM bibliographic_production
        WHERE 
        type = 'WORK_IN_EVENT'
        AND id NOT IN
        (SELECT reference_id FROM embeddings.event)
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.event (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.PORT,
            )

    print("ETAPA 6")
    SCRIPT_SQL = """
        SELECT id, title 
        FROM patent
        WHERE 
        id NOT IN
        (SELECT reference_id FROM embeddings.patent)
        """
    registry = sgbdSQL.consultar_db(SCRIPT_SQL)
    data_frame = pd.DataFrame(registry, columns=["id", "text"])

    for index, data in data_frame.iterrows():
        if data["text"]:
            print(data["text"])
            total_tokens = count_tokens(data["text"])
            price = get_custos(0)
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.patent (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(
                sql=script_sql,
                database=settings.MARIA_DATABASE_NAME,
                password=settings.MARIA_DATABASE_PASSWORD,
                host=settings.MARIA_DATABASE_HOST,
                port=settings.PORT,
            )
