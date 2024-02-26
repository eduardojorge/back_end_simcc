import pandas as pd
from Dao import sgbdSQL
import project


project.project_env = "4"
researcher_id = str()
graduate_program = str()


filter_researcher = str()
filter_graduate_program = str()

if researcher_id:
    filter_researcher = f"WHERE b.researcher_id = ''{researcher_id}''"
elif graduate_program:
    filter_graduate_program = f"""
    JOIN
        graduate_program_researcher gpr ON
            b.researcher_id = gpr.researcher_id
    WHERE gpr.graduate_program_id = ''{graduate_program}''
    """

script_sql = f"""
    SELECT
        translate(unaccent(LOWER(b.title)),''-\\.:;,'', '' '')::tsvector  
    FROM 
        bibliographic_production b
    {filter_researcher}
    {filter_graduate_program}
    """

script_sql = f"""
        SELECT 
            ndoc AS qtd,
            INITCAP(word) AS term
        FROM 
            ts_stat('{script_sql}')
        WHERE 
            CHAR_LENGTH(word)>3 
            AND word != 'para'
        ORDER BY 
            ndoc DESC 
        FETCH FIRST 20 ROWS ONLY;
        """

print(script_sql)

reg = sgbdSQL.consultar_db(script_sql)

# data_frame = pd.DataFrame(reg, columns=["qtd", "term"])

# print(data_frame)
