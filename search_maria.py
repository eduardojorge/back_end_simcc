from Dao import sgbdSQL
from bd_maria import get_embeddings
import pandas as pd


def term_search(query):
    embedding = get_embeddings(query)
    sql = f"""
        SELECT
            researcher_id,
            1 - (abstract_embeddings <=> '{embedding}') AS cosine_similarity
        FROM researcher_embeddings
        ORDER BY cosine_similarity desc
        LIMIT 10;
        """

    results = sgbdSQL.consultar_db(sql)
    data_frame = pd.DataFrame(
        results, columns=['researcher_id', 'proximidade'])
    print(tuple(data_frame['researcher_id'].to_list()))


if __name__ == "__main__":
    query = input("query: ")
    term_search(query)
