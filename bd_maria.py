from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv
import tiktoken
import os
from Dao import sgbdSQL
import pandas as pd


def get_embeddings(text):
    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-large", openai_api_key=os.getenv("OPENAI_API_KEY")
    )
    embeddings = embeddings_model.embed_query(text)
    return embeddings


def count_tokens(texto):
    tokenizer = tiktoken.get_encoding("cl100k_base")
    return tokenizer.encode(texto)


def get_custos(total_tokens):
    custos = total_tokens / 1000 * 0.0001
    return custos


if __name__ == "__main__":
    load_dotenv()

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.abstract (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.article (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.article_abstract (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.book (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.event (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)

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
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data["text"])
            script_sql = f"""
                INSERT INTO embeddings.patent (reference_id, embeddings, price)
                VALUES ('{data['id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)
