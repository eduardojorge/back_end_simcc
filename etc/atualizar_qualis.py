import Dao.sgbdSQL as sgbdSQL
import pandas as pd


def verficar_qualis_db(issn, extrato):
    sql = """SELECT   pm.name as name,qualis from 
         periodical_magazine pm WHERE translate(issn,'-','')='%s' AND qualis!='%s'
         """ % (
        issn.replace("-", ""),
        extrato,
    )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["name", "qualis"])
    if len(df_bd.index) >= 1:
        print(df_bd["name"].iloc[0])
        print("------------------------")
        print(df_bd["qualis"].iloc[0])
        print(extrato)
        print("------------------------")


def verficar_revista_nao_cadastrada_db(issn):
    sql = """

        SELECT   pm.name as name,qualis from 
         periodical_magazine pm WHERE translate(issn,'-','')='%s' 
         """ % (
        issn.replace("-", "")
    )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["name", "qualis"])
    if len(df_bd.index) == 0:
        print("------------------------")

        print(issn)
        print("------------------------")


def atualizar_qualis_bd(issn, extrato):
    sql = """
        
        
        UPDATE  periodical_magazine p SET qualis='%s'
        WHERE translate(issn,'-','')='%s'
         """ % (
        extrato,
        issn.replace("-", ""),
    )

    print(sql)

    sgbdSQL.execScript_db(sql)


df = pd.read_excel(
    r"C:\Users\Emjorge\OneDrive\simccv_cimatec_v4\operationalData\qualis.xlsx"
)
print(df)


ISSN = 0
EXTRATO = 3

for i, infos in df.iterrows():
    # print(infos[ISSN])
    # print(infos[1])
    # print(infos[2])
    # print(infos[EXTRATO])
    verficar_qualis_db(infos[ISSN], infos[EXTRATO])
    # verficar_revista_nao_cadastrada_db(infos[ISSN])
# atualizar_qualis_bd(infos[ISSN],infos[EXTRATO])


# break
