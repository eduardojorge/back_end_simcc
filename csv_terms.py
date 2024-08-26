from dotenv import load_dotenv

load_dotenv()
import firebase_admin
from firebase_admin import credentials, firestore

from Dao import sgbdSQL
import pandas as pd

cred = credentials.Certificate("cert.json")

firebase_admin.initialize_app(cred)
db = firestore.client()

script_sql = """
    SELECT term, frequency, type_, '0' as great_area, unaccent(LOWER(term)) AS term_normalize
    FROM public.research_dictionary d
    WHERE term ~ '^[^0-9]+$'
        AND CHAR_LENGTH(d.term) >= 4
        AND frequency >= 8
        AND type_ NOT IN ('BOOK', 'PATENT')

    UNION

    SELECT term, frequency, type_, '0', unaccent(LOWER(term)) AS term_normalize
    FROM public.research_dictionary d
    WHERE term ~ '^[^0-9]+$'
        AND CHAR_LENGTH(d.term) >= 4
        AND type_ IN ('BOOK', 'PATENT')

    UNION

    SELECT LOWER(NAME), 1, 'NAME', '0', '0'
    FROM researcher

    UNION


    SELECT 
        area,
        1,
        'AREA',
        great_area,
        unaccent(LOWER(great_area)) AS term_normalize
    FROM 
        (SELECT
            LOWER(TRIM(STRING_TO_TABLE(SPLIT_PART(area_specialty, '|', 1), ';'))) as area,
            LOWER(TRIM(SPLIT_PART(area_specialty, '|', 2), ';')) as great_area
        FROM public.researcher_production
        ORDER BY area)
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

termos_busca_ref = db.collection("termos_busca")

for item in termos_busca.to_dict(orient="records"):
    termos_busca_ref.add(item)

print("Documentos adicionados com sucesso.")
