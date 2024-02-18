import Dao.sgbdSQL as db
import pandas as pd
import project

project.project_env = "4"


data_frame = pd.read_csv("Files/graduate_program_old_simcc.csv")

for Index, Data in data_frame.iterrows():
    script_sql = f"""
        INSERT INTO public.graduate_program(
	    graduate_program_id, code, name, area, modality, type, rating, institution_id, state, city, url_image, region, sigla, latitude, longitude)
	    VALUES ('{Data['graduate_program_id']}', '{Data['code']}', '{Data['name']}', '{Data['area']}', '{Data['modality']}', '{Data['type']}', '{Data['rating']}', '{Data['institution_id']}', '{Data['state']}', '{Data['city']}', '{Data['url_image']}', '{Data['region']}', '{Data['sigla']}', '{Data['latitude']}', '{Data['longitude']}');
        """
    db.execScript_db(script_sql)


data_frame = pd.read_csv("Files/graduate_program_researcher_old_simcc.csv")

for Index, Data in data_frame.iterrows():
    script_sql = f"""
        INSERT INTO public.graduate_program_researcher(
	    graduate_program_id, researcher_id, year, type_)
	    VALUES ('{Data['graduate_program_id']}', (SELECT id FROM researcher WHERE lattes_id = '{Data['lattes_id']}'), {Data['year']}, '{Data['type_']}');
        """
    db.execScript_db(script_sql)
