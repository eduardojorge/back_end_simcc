import pandas as pd
from Dao import sgbdSQL

data_frame = pd.read_csv("Files/lattes_painel.csv")

for i, data in data_frame.iterrows():
    lattes_url = data["LATTES"]

    if pd.notna(lattes_url):
        last_slash_index = lattes_url.rfind("/")

        if last_slash_index != -1:
            print(lattes_url[last_slash_index + 1 :])

    SCRIPT_SQL = f"""
        INSERT INTO public.researcher (name, lattes_id, institution_id)
        VALUES ('...', '{lattes_url[last_slash_index + 1 :]}', '083a16f0-cccf-47d2-a676-d10b8931f66a');
        """
    sgbdSQL.execScript_db(database="barema_admin", sql=SCRIPT_SQL)
