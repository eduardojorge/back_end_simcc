import zipfile
import requests
import Dao.sgbdSQL as sgbdSQL
import logging
import pandas as pd
import os
from datetime import datetime
from zeep import Client
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


load_dotenv(override=True)

client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")

logger = logging.getLogger(__name__)


def get_data_att(id: str, alternative_cnpq_service: bool) -> datetime:
    url = (
        f"https://simcc.uesc.br:8080/getDataAtualizacaoCV?lattes_id={id}"
        if alternative_cnpq_service
        else None
    )
    resultado = (
        requests.get(url, verify=False).json()
        if alternative_cnpq_service
        else client.service.getDataAtualizacaoCV(id)
    )
    resultado = resultado or "01/01/0001 00:00:00"
    return datetime.strptime(resultado, "%d/%m/%Y %H:%M:%S")


def last_update(id: str) -> datetime:
    script_sql = f"SELECT last_update FROM researcher WHERE lattes_id = '{id}';"
    registry = sgbdSQL.consultar_db(script_sql)
    return (
        registry[0][0]
        if registry
        else datetime.strptime("01/01/0001 00:00:00", "%d/%m/%Y %H:%M:%S")
    )


def get_id_cnpq(name: str = "", date: str = "", CPF: str = "") -> str:
    return client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )


def save_cv(id: str, dir: str, alternative_cnpq_service: bool):
    if get_data_att(
        id=id, alternative_cnpq_service=alternative_cnpq_service
    ) <= last_update(id):
        msg = f"Currículo já está atualizado id: {id}"
        print(msg)
        logger.debug(msg)
        return

    msg = f"Currículo não atualizado id: {id}"
    print(msg)
    logger.debug(msg)

    try:
        url = (
            f"https://simcc.uesc.br:8080/getCurriculoCompactado?lattes_id={id}"
            if alternative_cnpq_service
            else None
        )
        resultado = (
            requests.get(url, verify=False).content
            if alternative_cnpq_service
            else client.service.getCurriculoCompactado(id)
        )

        file_path = os.path.join(dir, "zip", f"{id}.zip")
        with open(file_path, "wb") as arquivo:
            arquivo.write(resultado)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(dir)
            zip_ref.extractall(os.path.join(dir, "atual"))

        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as E:
        print(E)
        logger.error("\nErro: Currículo não existe")


def get_researcher_adm_simcc() -> pd.DataFrame:
    script_sql = "SELECT name, lattes_id FROM researcher;"
    registry = sgbdSQL.consultar_db(sql=script_sql, database=os.environ["ADM_DATABASE"])
    df = pd.DataFrame(registry, columns=["name", "lattes_id"])
    return df


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
    logger = logging.getLogger()

    if os.getenv("ALTERNATIVE_CNPQ_SERVICE", False):
        print("baixando curriculos pelo Tupi")

    dir = f'{os.environ["JADE_EXTRATOR_FOLTER"]}/config/projects/Jade-Extrator-Hop/metadata/dataset/xml/'

    for Files in os.listdir(dir):
        try:
            os.remove(os.path.join(dir, Files))
        except Exception:
            logger.error("Erro 003: Directory")
    logger.debug("Arquivos XML removidos")

    quant_curriculos = 0

    df = get_researcher_adm_simcc()

    for Index, Data in df.iterrows():
        print(f"Curriculo número: {quant_curriculos}")
        print(f"ID do pesquisador: {Data['lattes_id']}")

        lattes_id = str(Data["lattes_id"]).zfill(16)
        save_cv(lattes_id, dir, os.getenv("ALTERNATIVE_CNPQ_SERVICE", False))
        quant_curriculos += 1

    logger.debug(f"FIM: {str(quant_curriculos)}")
    print(f"FIM, Quantidade de curriculos processados: {str(quant_curriculos)}")
