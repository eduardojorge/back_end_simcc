import pandas as pd

import Dao.sgbdSQL as sgbdSQL


def verficar_qualis_db(issn: str, extrato: str) -> None:
    script_sql = f"""
        SELECT
            pm.name AS name,
            qualis
        FROM 
            periodical_magazine pm 
        WHERE 
            translate(issn, '-', '') = '{issn.replace("-", "")}'
            AND qualis != '{extrato}';        
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(registry, columns=["name", "qualis"])

    if len(data_frame_bd.index) >= 1:
        print(
            f"""
            Nome: {data_frame_bd['name'].iloc[0]}\n
            Qualis: {data_frame_bd['qualis'].iloc[0]}\n
            Extrato: {extrato}
            """
        )


def verficar_revista_nao_cadastrada_db(issn):
    script_sql = f"""
        SELECT
            pm.name AS name,
            qualis
        FROM 
            periodical_magazine pm 
        WHERE 
            translate(issn, '-', '') = '{issn.replace("-", "")}';
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(registry, columns=["name", "qualis"])

    if len(data_frame_bd.index) == 0:
        print(f"ISSN: {issn}")


def att_qualis_bd(issn, extrato):
    script_sql = f"""
        UPDATE periodical_magazine p
        SET qualis = '{extrato}'
        WHERE translate(issn, '-', '') = '{issn.replace("-", "")}';
        """
    sgbdSQL.execScript_db(script_sql)


if __name__ == "__main__":
    df = pd.read_excel("Files/qualis.xlsx")

    ISSN = 0
    EXTRATO = 3
    for Index, Data in df.iterrows():
        verficar_qualis_db(Data[ISSN], Data[EXTRATO])
