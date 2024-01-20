import Dao.sgbdSQL as sgbdSQL
import pandas as pd


import project as project_

project_.project_ = "1"


def insert_student_graduate_program_db(lattes_id, graduate_program_id, year, type_):
    print(lattes_id)

    type_ = "EFETIVO"
    sql = """INSERT into public.graduate_program_student (lattes_id,graduate_program_id,year,type_) 
                values('%s',%s,%s,'%s');""" % (
        lattes_id,
        graduate_program_id,
        year,
        type_,
    )
    print(sql)

    sgbdSQL.execScript_db(sql)


# df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
df = pd.read_excel(r"files/alunos_getec_doutorado.xlsx")
print(df)
ID_LATTES = 0

x = 0

for i, infos in df.iterrows():
    print("teste x " + str(infos[ID_LATTES]))

    insert_student_graduate_program_db(str(infos[ID_LATTES]), 4, 2023, "EFETIVO")

    x = x + 1
# print(df)
print("Fim " + str(x))


# print(termFlowSQL.list_researchers_originals_words_db("biomassa","","ABSTRACT","or",""))
# print(areaFlowSQL.lista_researcher_patent_db("biomassa","",""))
