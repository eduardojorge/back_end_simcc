import firebase_admin
from firebase_admin import credentials, firestore

from Dao import sgbdSQL
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

cred = credentials.Certificate("cert.json")

firebase_admin.initialize_app(cred)
db = firestore.client()

script_sql = """
    SELECT
        term,
        frequency,
        type_,
        '0' AS great_area,
        unaccent(LOWER(term)) AS term_normalize
    FROM
        public.research_dictionary d
    WHERE
        term ~ '^[^0-9]+$'
        AND CHAR_LENGTH(d.term) >= 4
        AND frequency >= 8
        AND type_ NOT IN ('BOOK', 'PATENT')

    UNION

    SELECT
        term,
        frequency,
        type_,
        '0',
        unaccent(LOWER(term)) AS term_normalize
    FROM
        public.research_dictionary d
    WHERE
        term ~ '^[^0-9]+$'
        AND CHAR_LENGTH(d.term) >= 4
        AND type_ IN ('BOOK', 'PATENT')

    UNION

    SELECT
        unaccent(LOWER(name)),
        1,
        'NAME',
        '0',
        '0'
    FROM
        researcher

    UNION

    SELECT
        AREA,
        1,
        'AREA',
        great_area,
        unaccent(LOWER(great_area)) AS term_normalize
    FROM
        (
            SELECT
                LOWER(TRIM(STRING_TO_TABLE(SPLIT_PART(area_specialty, '|', 1), ';'))) AS AREA,
                LOWER(TRIM(SPLIT_PART(area_specialty, '|', 2), ';')) AS great_area
            FROM
                public.researcher_production
            ORDER BY
                AREA
        ) AS subquery

    UNION

    SELECT
        name,
        '1',
        'NAME',
        '0',
        unaccent(LOWER(name))
    FROM 
        researcher
    """

registry = sgbdSQL.consultar_db(script_sql)

termos_busca = pd.DataFrame(
    registry,
    columns=[
        "term",
        "frequency",
        "type_",
        "great_area",
        "term_normalize",
    ],
)


termos_busca_ref = db.collection(os.getenv("FIREBASE_COLLECTION", "termos_busca"))
docs = termos_busca_ref.stream()
for doc in docs:
    doc.reference.delete()

for item in termos_busca.to_dict(orient="records"):
    print(item)
    termos_busca_ref.add(item)

print("Documentos adicionados com sucesso.")
