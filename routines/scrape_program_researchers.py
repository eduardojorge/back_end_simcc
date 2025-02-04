import pandas as pd
import psycopg
import unidecode

from simcc.repositories import conn

researchers = pd.read_csv('storage/programs_researcher.csv')


for _, researcher in researchers.iterrows():
    researcher['name'] = (
        unidecode.unidecode(researcher['name']).replace(' ', '').lower()
    )

    SCRIPT_SQL = """
        SELECT id FROM researcher
        WHERE LOWER(REPLACE(unaccent(name), ' ', '')) = %(name)s
        """

    researcher_data = researcher.to_dict()
    if db_data := conn.select(SCRIPT_SQL, researcher_data, one=True):
        researcher_data = {**db_data, **researcher_data}
        try:
            SCRIPT_SQL = """
                INSERT INTO graduate_program_researcher (graduate_program_id, researcher_id, year, type_)
                SELECT graduate_program_id, %(id)s, ARRAY[2024,2025,2023,2022,2021], %(type_)s
                FROM graduate_program
                WHERE code = %(code)s
                """
            conn.exec(SCRIPT_SQL, researcher_data)
        except psycopg.errors.UniqueViolation:
            print('Unique violation: ', researcher_data['id'])
        except Exception as e:
            print(e)
