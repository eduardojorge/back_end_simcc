import requests
import Dao.sgbdSQL as sgbdSQL
import logging
import pandas as pd
import os
from datetime import datetime
import urllib3
from config import settings
from zeep import Client
import zipfile

client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def CNPq_att(id: str, proxy: bool) -> datetime:
    if proxy:
        URL = f"https://simcc.uesc.br/api/getDataAtualizacaoCV?lattes_id={id}"
        resultado = requests.get(URL, verify=False).json()
    else:
        resultado = client.service.getDataAtualizacaoCV(id)
    return datetime.strptime(resultado or "01/01/0001 00:00:00", "%d/%m/%Y %H:%M:%S")


def last_update(lattes_id: str):
    lattes_id = lattes_id.zfill(16)
    script_sql = f"SELECT last_update FROM researcher WHERE lattes_id = '{lattes_id}';"
    registry = sgbdSQL.consultar_db(script_sql)
    return registry[0][0] if registry else datetime.min


def get_id_cnpq(name: str = "", date: str = "", CPF: str = "") -> str:
    return client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )


def save_cv(lattes_id: str, dir: str, proxy: bool):
    lattes_date = CNPq_att(id=lattes_id, proxy=proxy)
    if lattes_date <= last_update(lattes_id):
        msg = f"{lattes_id} já está atualizado id"
        logger.debug(msg)
        print(msg)
        # return

    msg = f"{lattes_id} não atualizado id: "
    logger.debug(msg)
    print(msg)

    try:
        if proxy:
            URL = f"https://simcc.uesc.br/api/getCurriculoCompactado?lattes_id={lattes_id}"
            response = requests.get(URL, verify=False).content
        else:
            response = client.service.getCurriculoCompactado(lattes_id)

        file_path = os.path.join(dir, "zip", f"{lattes_id}.zip")
        with open(file_path, "wb") as cv:
            cv.write(response)

        with zipfile.ZipFile(file_path, "r") as cv_zip:
            cv_zip.extractall(dir)
            cv_zip.extractall(os.path.join(dir, "atual"))

        if os.path.exists(file_path):
            os.remove(file_path)

    except Exception as E:
        print(E)
        logger.error("\nErro: Currículo não existe")


def list_researchers():
    SCRIPT_SQL = """
        SELECT name, lattes_id
        FROM researcher;
        """
    registry = sgbdSQL.consultar_db(sql=SCRIPT_SQL, database=settings.ADM_DATABASE)
    data_frame = pd.DataFrame(registry, columns=["name", "lattes_id"])
    return data_frame


if __name__ == "__main__":
    log_dir = "Files/log"
    log_file = "soap_lattes_adm.log"

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(
        filename=os.path.join(log_dir, log_file),
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )
    logger = logging.getLogger(__name__)

    path = f"{settings.JADE_EXTRATOR_FOLTER}/config/projects/Jade-Extrator-Hop/metadata/dataset/xml/"

    for folder in ["atual", "zip"]:
        other_path = os.path.join(path, folder)
        if not os.path.exists(other_path):
            os.makedirs(other_path)

    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    qtd = 0

    df_researchers = list_researchers()

    for _, data in df_researchers.iterrows():
        print(f"Curriculo número: {qtd}")
        print(f"ID do pesquisador: {data['lattes_id']}")

        lattes_id = str(data["lattes_id"]).zfill(16)
        try:
            save_cv(lattes_id, path, settings.ALTERNATIVE_CNPQ_SERVICE)
        except Exception:
            print(f"Erro encontrado!!! {data['lattes_id']}")
        qtd += 1

    logger.debug(f"FIM: {str(qtd)}")
    print(f"FIM, Quantidade de curriculos processados: {str(qtd)}")
