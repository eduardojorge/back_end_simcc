from Dao import sgbdSQL
import pandas as pd


from firebase_admin import credentials, firestore
import firebase_admin
from config import settings


def terms_dataframe() -> pd.DataFrame:
    SCRIPT_SQL = """
        SELECT term, frequency, type_, '0' AS great_area, unaccent(LOWER(term)) AS term_normalize
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

        SELECT unaccent(LOWER(name)), 1, 'NAME', '0', unaccent(LOWER(name)) AS term_normalize
        FROM researcher

        UNION

        SELECT AREA, 1, 'AREA', great_area, unaccent(LOWER(great_area)) AS term_normalize
        FROM (SELECT LOWER(TRIM(STRING_TO_TABLE(SPLIT_PART(area_specialty, '|', 1), ';'))) AS AREA,
                        LOWER(TRIM(STRING_TO_TABLE(SPLIT_PART(area_specialty, '|', 2), ';'))) AS great_area
                FROM public.researcher_production
                ORDER BY AREA) AS subquery

        UNION

        SELECT name, '1', 'NAME', '0', unaccent(LOWER(name)) AS term_normalize
        FROM researcher
        """
    result = sgbdSQL.consultar_db(SCRIPT_SQL)
    columns = ["term", "frequency", "type_", "great_area", "term_normalize"]
    terms = pd.DataFrame(result, columns=columns)
    return terms


if __name__ == "__main__":
    cred = credentials.Certificate("cert.json.json")

    firebase_admin.initialize_app(credential=cred)
    db = firestore.client()

    dictionary = db.collection(settings.FIREBASE_COLLECTION)
    docs = dictionary.stream()
    for doc in docs:
        print(f"Deletando [{doc.get('type_')}]")
        doc.reference.delete()

    for item in terms_dataframe().to_dict(orient="records"):
        print(f"Adicionando [{item.get('type_')}]")
        dictionary.add(item)
