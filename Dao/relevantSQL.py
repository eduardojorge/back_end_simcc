import pandas as pd
import Dao.sgbdSQL as db

def post_image(production_id, type_, has_image):
    SCRIPT_SQL = """
        INSERT INTO public.relevant_production (production_id, type, has_image)
        VALUES (%s, %s, %s)
        ON CONFLICT (production_id, type)
        DO UPDATE SET has_image = true;
        """
    db.execScript_db(SCRIPT_SQL, [production_id, type_, has_image])
    

def get_relevant_list():
    SCRIPT_SQL = """
        SELECT production_id, type, has_image, created_at
        FROM public.relevant_production;
        """
    
    result = db.consultar_db(SCRIPT_SQL)
    df = pd.DataFrame(result, columns=["production_id", "type", 'has_image', "created_at"])
    return df.to_dict(orient='records')

def delete_image(production_id, type_):
    SCRIPT_SQL = """
        DELETE FROM public.relevant_production
        WHERE production_id = %s AND type = %s;
        """
    db.execScript_db(SCRIPT_SQL, [production_id, type_])