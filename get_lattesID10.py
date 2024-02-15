import urllib3
import time
import requests
import Dao.sgbdSQL as sgbdSQL
import pandas as pd


def getLattesId10(lattes_id: str) -> str:
    http = urllib3.PoolManager()
    try:

        url = "http://lattes.cnpq.br/" + lattes_id
        response = requests.get(url, verify=False, timeout=30)
        print(response.url)

        page = requests.get(url)
        print(page)
        result = http.request("GET", url, timeout=30)
        print(result)

        position = str(result.data).rfind('<input type="hidden" name="id" value="')
        start_position = position + 38
        end_position = position + 48

        code = str(result.data)[start_position:end_position]
    except:
        print("Connection refused by the server..")
        print("Let me sleep for 5 seconds")
        print("ZZzzzz...")
        time.sleep(5)
        print("Was a nice sleep, now let me continue...")
        print(url)
        result = http.request("GET", url, verify=False)
        print(result)

    return code


# print (getLattesId10(lattes_id="6716225567627323"))


def lattes_10_researcher_frequency_db():

    reg = sgbdSQL.consultar_db(
        "SELECT r.id as id, r.lattes_id as lattes from researcher r "
    )
    #'where id=\'35e6c140-7fbb-4298-b301-c5348725c467\''+
    #' OR id=\'c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf\' ')
    df_bd = pd.DataFrame(reg, columns=["id", "lattes"])
    print(df_bd)
    for i, infos in df_bd.iterrows():

        lattes_10_id = ""
        lattes_10_id = getLattesId10(infos.lattes)
        sql = f"""
            UPDATE researcher set lattes_10_id='{lattes_10_id}' where id='{infos.id}' 
            """

        print(sql)

        sgbdSQL.execScript_db(sql)


lattes_10_researcher_frequency_db()
