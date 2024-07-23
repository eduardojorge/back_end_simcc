import pandas as pd
from Dao import sgbdSQL
from unidecode import unidecode

for index, data in pd.read_csv("Files/grupos_pesquisa.csv").iterrows():
    leader_one_id = "e611433a-fe07-4af7-913a-82d9dc040d87"
    leader_two_id = "e611433a-fe07-4af7-913a-82d9dc040d87"

    script_sql = f"""
        SELECT id 
        FROM researcher 
        WHERE unaccent(name) ILIKE unaccent('{str(data['Lider 1']).replace("'", ' ')}')
        LIMIT 1
        """
    reg = sgbdSQL.consultar_db(script_sql)
    if reg:
        leader_one_id = reg[0][0]
    script_sql = f"""
        SELECT id 
        FROM researcher 
        WHERE unaccent(name) ILIKE unaccent('{str(data['Lider 2']).replace("'", ' ')}')
        LIMIT 1
        """
    reg = sgbdSQL.consultar_db(script_sql)
    if reg:
        leader_two_id = reg[0][0]

    script_sql = f"""
        INSERT INTO research_group_dgp (name, institution, leader_one, leader_one_id, leader_two, leader_two_id, area)
        VALUES ('{data['Nome'].replace("'", ' ')}', '{data['Instituição']}', '{data['Lider 1']}', '{leader_one_id}', '{data['Lider 2']}', '{leader_two_id}','{data['Área']}')
        """
    sgbdSQL.execScript_db(script_sql)
