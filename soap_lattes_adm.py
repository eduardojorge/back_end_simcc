import zipfile
import requests
import Dao.sgbdSQL as sgbdSQL
import logging
import pandas as pd
import os
from datetime import datetime
from zeep import Client
from dotenv import load_dotenv

load_dotenv(override=True)


def get_data_att(id: str, alternative_cnpq_service: bool) -> datetime:
    if alternative_cnpq_service:
        url = f"https://simcc.uesc.br:8080/getDataAtualizacaoCV?lattes_id={id}"
        resultado = requests.get(url, verify=False).json()
    else:
        resultado = client.service.getDataAtualizacaoCV(id)

    if resultado == None:
        resultado = "01/01/0001 00:00:00"
    return datetime.strptime(resultado, "%d/%m/%Y %H:%M:%S")


def last_update(id: str):
    script_sql = f"SELECT last_update FROM researcher WHERE lattes_id = '{id}';"

    registry = sgbdSQL.consultar_db(script_sql)

    if registry:
        return registry[0][0]
    resultado = "01/01/0001 00:00:00"
    return datetime.strptime(resultado, "%d/%m/%Y %H:%M:%S")


def get_id_cnpq(name: str = str(), date: str = str(), CPF: str = str()):
    resultado = client.service.getIdentificadorCNPq(nomeCompleto=name,
                                                    dataNascimento=date,
                                                    cpf=CPF)
    if resultado:
        return resultado


def save_cv(id: str, dir: str, alternative_cnpq_service: bool):

    if get_data_att(
            id=id,
            alternative_cnpq_service=alternative_cnpq_service) <= last_update(
                id):
        msg = f"Currículo já está atualizado id: {str(id)}"
        print(msg)
        logger.debug(msg)
        return

    msg = f"Currículo não atualizado id: {str(id)}"
    print(msg)
    logger.debug(msg)

    try:
        if alternative_cnpq_service:
            url = f"https://simcc.uesc.br:8080/getCurriculoCompactado?lattes_id={id}"
            resultado = requests.get(url, verify=False).content
        else:
            resultado = client.service.getCurriculoCompactado(id)
        arquivo = open(f"{dir}/zip/{id}.zip", "wb")
        arquivo.write(resultado)
        arquivo.close()

        with zipfile.ZipFile(f"{dir}/zip/{id}.zip", "r") as zip_ref:
            zip_ref.extractall(dir)
            zip_ref.extractall(f"{dir}/atual")
            if os.path.exists(f"{id}.zip"):
                os.remove(f"{id}.zip")
    except Exception as E:
        print(E)
        logger.error("\nErro: Currículo não  existe")


def get_researcher_adm_simcc():
    script_sql = """
        SELECT
            name,
            lattes_id
        FROM
            researcher;
        """
    registry = sgbdSQL.consultar_db(script_sql, os.environ["ADM_DATABASE"])

    df = pd.DataFrame(registry, columns=["name", "lattes_id"])

    return df


if __name__ == "__main__":
    if os.getenv("ALTERNATIVE_CNPQ_SERVICE", False):
        print("baixando curriculos pelo Tupi")

    dir = f"{os.environ['JADE_EXTRATOR_FOLTER']}metadata/dataset/xml"

    client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
    Log_Format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(
        filename="Log/soap_lattes_adm.log",
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )
    logger = logging.getLogger()
    logger.debug("Inicio")

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
    print(
        f"FIM, Quantidade de curriculos processados: {str(quant_curriculos)}")
