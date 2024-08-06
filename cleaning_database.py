from Dao import sgbdSQL
import pandas as pd

script_sql = """
    SELECT 
        researcher_id, 
        title, 
        COUNT(*), 
        year 
    FROM 
        bibliographic_production 
    WHERE 
        type = 'ARTICLE' 
    GROUP BY 
        title, 
        researcher_id, 
        year 
        HAVING COUNT(*) > 1
    """

registry = sgbdSQL.consultar_db(script_sql)

df = pd.DataFrame(registry, columns=["researcher_id", "title", "count", "year"])
for id, data in df.iterrows():
    script_sql = f"""
        SELECT
            b.id
        FROM
            bibliographic_production b
        WHERE
            title = '{data.title}' 
            AND researcher_id = '{data.researcher_id}' 
        OFFSET 1
        """
    registry = sgbdSQL.consultar_db(script_sql)

    df_2 = pd.DataFrame(registry, columns=["b_id"])

    for b_id, data in df_2.iterrows():
        script_sql = f"""
        DELETE FROM bibliographic_production_article WHERE bibliographic_production_id = '{data.b_id}';
        DELETE FROM bibliographic_production WHERE id = '{data.b_id}';
        """
        sgbdSQL.execScript_db(script_sql)


script_sql = """
SELECT bibliographic_production_id, COUNT(*) FROM bibliographic_production_article GROUP BY bibliographic_production_id HAVING COUNT(*) > 1
"""
registry = sgbdSQL.consultar_db(script_sql)
df = pd.DataFrame(registry, columns=["bibliographic_production_id", "count"])

for index, data in df.iterrows():
    script_sql = f"SELECT id FROM bibliographic_production_article WHERE bibliographic_production_id = '{data.bibliographic_production_id}' OFFSET 1"
    registry_2 = sgbdSQL.consultar_db(script_sql)
    print(registry_2)
    df_2 = pd.DataFrame(registry_2, columns=["id"])
    for index_2, data_2 in df_2.iterrows():
        script_sql = f"""DELETE FROM bibliographic_production_article WHERE id = '{data_2.id}' """
        sgbdSQL.execScript_db(script_sql)
