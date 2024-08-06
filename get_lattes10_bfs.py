import requests
from bs4 import BeautifulSoup
import Dao.sgbdSQL as sgbdSQL
import pandas as pd


def getLattesId10(lattes_id: str) -> str:
    headers = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "Origin": "http://buscatextual.cnpq.br",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/93.0.4577.63 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
        "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Referer": "http://buscatextual.cnpq.br/buscatextual/busca.do?metodo=apresentar",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    url = f"http://lattes.cnpq.br/{lattes_id}"

    response = requests.get(url, headers=headers, stream=True)
    with open("name", "wb") as f:
        f.write(response.content)

    soup = BeautifulSoup(response.content, "html.parser")

    input_element = soup.find("input", {"type": "hidden", "name": "id"})
    if input_element is not None:
        code = input_element.get("value", "")[:10]
        return code

    return None


def update_lattes_id_10():

    script_sql = """
        SELECT 
            r.id as id, 
            r.lattes_id as lattes 
        FROM 
            researcher r
        WHERE
            r.lattes_10_id IS NULL;
        """

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(reg, columns=["id", "lattes"])

    for Index, infos in data_frame.iterrows():

        lattes_10_id = getLattesId10(infos["lattes"])

        script_sql = f"""
            UPDATE researcher
            SET lattes_10_id = '{lattes_10_id}' WHERE id = '{infos['id']}';
            """

        print(f"Sucesso com o pesquisador número: {Index}")
        sgbdSQL.execScript_db(script_sql)


update_lattes_id_10()
