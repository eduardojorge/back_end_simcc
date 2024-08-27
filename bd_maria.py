from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv
import tiktoken
import os
from Dao import sgbdSQL
import pandas as pd


def get_embeddings(text):
    """
    #Função responsável por gerar os embeddings
    """
    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-large", openai_api_key=os.getenv("OPENAI_API_KEY"))
    embeddings = embeddings_model.embed_query(text)
    return embeddings


def count_tokens(texto):
    """
    #Função responsável por contar a quantidade de tokens
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")
    return tokenizer.encode(texto)


def get_custos(total_tokens):
    """
    #Função responsável por calcular os custos
    """
    custos = total_tokens / 1000 * 0.0001
    return custos


if __name__ == "__main__":
    load_dotenv()
    registry = sgbdSQL.consultar_db(
        'SELECT id, abstract FROM researcher WHERE id NOT IN (SELECT researcher_id FROM embeddings.abstract);')
    data_frame = pd.DataFrame(registry, columns=['researcher_id', 'abstract'])

    print('Processando resumos:')
    for index, data in data_frame.iterrows():
        if data['abstract']:
            print(data['abstract'])
            total_tokens = count_tokens(data['abstract'])
            price = get_custos(len(total_tokens))
            embeddings = get_embeddings(data['abstract'])
            script_sql = f"""
                INSERT INTO embeddings.abstract (researcher_id, embeddings, price)
                VALUES ('{data['researcher_id']}', '{embeddings}', '{price}')
                """
            sgbdSQL.execScript_db(script_sql)
