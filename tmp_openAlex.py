import os
from Dao import sgbdSQL as db

if __name__ == "__main__":
    for json_path in os.listdir("Files/openAlex_article"):
        if json_path.endswith(".json"):
            SCRIPT_SQL = f"""
                SELECT doi 
                FROM bibliographic_production_article
                WHERE id = '{json_path[:-5]}'
                """
            registry = db.consultar_db(SCRIPT_SQL)
            os.rename(f"Files/openAlex_article/{registry[0][0]}.json")

    for json_path in os.listdir("Files/openAlex_researcher"):
        if json_path.endswith(".json"):
            SCRIPT_SQL = f"""
                SELECT lattes_id 
                FROM researcher
                WHERE id = '{json_path[:-5]}'
                """
            registry = db.consultar_db(SCRIPT_SQL)
            os.rename(f"Files/openAlex_researcher/{registry[0][0]}.json")
