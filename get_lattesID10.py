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
        result = http.request("GET", url, timeout=30)
        position = str(result.data).rfind('<input type="hidden" name="id" value="')
        start_position = position + 38
        end_position = position + 48
        code = str(result.data)[start_position:end_position]
    except Exception as Error:
        print("Connection refused by the server..")
        print("Let me sleep for 5 seconds")
        print("ZZzzzz...")
        time.sleep(5)
        print("Was a nice sleep, now let me continue...")
        print(url)
        result = http.request("GET", url, verify=False)
        print(result)

    return code


def update_lattes_id_10():

    script_sql = """
        SELECT 
            r.id as id, 
            r.lattes_id as lattes 
        FROM 
            researcher r;
        """

    reg = sgbdSQL.consultar_db()

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
