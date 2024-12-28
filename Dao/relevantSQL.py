import pandas as pd
import Dao.sgbdSQL as db

def add_relevant_production(researcher_id, production_id, type_, has_image):
    SCRIPT_SQL = """
        INSERT INTO public.relevant_production (researcher_id, production_id, type, has_image)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (researcher_id, production_id, type)
        DO UPDATE SET has_image = true;
        """
    db.execScript_db(SCRIPT_SQL, [researcher_id, production_id, type_, has_image])
    

def get_relevant_list(researcher_id, type_):
    params = []
    SCRIPT_SQL = """
        SELECT production_id, type, has_image, created_at
        FROM public.relevant_production
        WHERE 1 = 1
        """
    
    if researcher_id:
        SCRIPT_SQL += "AND researcher_id = %s"
        params.append(researcher_id)

    if type_:
        SCRIPT_SQL += "AND type = %s"
        params.append(type_)

    result = db.consultar_db(SCRIPT_SQL, params)
    df = pd.DataFrame(result, columns=["production_id", "type", 'has_image', "created_at"])
    return df.to_dict(orient='records')

def delete_relevant_production(production_id, type_):
    SCRIPT_SQL = """
        DELETE FROM public.relevant_production
        WHERE production_id = %s AND type = %s;
        """
    db.execScript_db(SCRIPT_SQL, [production_id, type_])