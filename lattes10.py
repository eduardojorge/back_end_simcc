import requests
from bs4 import BeautifulSoup
import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import logging


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

    url = "http://lattes.cnpq.br/" + lattes_id
    # url="https://buscatextual.cnpq.br/buscatextual/download.do?metodo=apresentar&idcnpq=6716225567627323"
    response = requests.get(url, headers=headers, stream=True)
    with open("name", "wb") as f:
        f.write(response.content)

    soup = BeautifulSoup(response.content, "html.parser")

    input_element = soup.find("input", {"type": "hidden", "name": "id"})
    if input_element is not None:
        code = input_element.get("value", "")[:10]
        return code

    return None


def lattes_10_researcher_frequency_db(logger):

    reg = sgbdSQL.consultar_db(
        "SELECT r.id as id, r.lattes_id as lattes from researcher r "
    )
    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
    df_bd = pd.DataFrame(reg, columns=["id", "lattes"])
    # print (df_bd)
    for i, infos in df_bd.iterrows():

        lattes_10_id = ""
        lattes_10_id = getLattesId10(infos.lattes)
        sql = """
        UPDATE  researcher set lattes_10_id='%s' where id=\'%s\' """ % (
            lattes_10_id,
            infos.id,
        )

        print(sql)
        logger.debug(sql)

        sgbdSQL.execScript_db(sql)


print("Passo I")
