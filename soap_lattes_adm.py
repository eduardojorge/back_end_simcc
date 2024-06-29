from dotenv import load_dotenv

load_dotenv(override=True)

from zeep import Client
from datetime import datetime
import os
import pandas as pd
import logging
from datetime import datetime
import Dao.sgbdSQL as sgbdSQL
import project as project
import requests
import project
import zipfile


def get_data_att(id: str, cnpq_service: bool = True) -> datetime:
    if cnpq_service:
        resultado = client.service.getDataAtualizacaoCV(id)
    else:
        resultado = requests.get(
            f"simcc.uesc.br:8080/getDataAtualizacaoCV?lattes_id={id}"
        )
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
    resultado = client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )
    if resultado != None:
        return resultado


def save_cv(id, dir, cnpq_service: bool = True):

    if get_data_att(id=id, cnpq_service=cnpq_service) <= last_update(id):
        msg = f"Currículo já está atualizado id: {str(id)}"
        print(msg)
        logger.debug(msg)
        return

    msg = f"Currículo não atualizado id: {str(id)}"
    print(msg)
    logger.debug(msg)

    try:
        if cnpq_service:
            resultado = client.service.getCurriculoCompactado(id)
        else:
            resultado = requests.get(
                f"simcc.uesc.br:8080/getCurriculoCompactado?lattes_id={id}"
            )
        arquivo = open(f"{dir}/zip/{id}.zip", "wb")
        arquivo.write(resultado)
        arquivo.close()

        with zipfile.ZipFile(f"{dir}/zip/{id}.zip", "r") as zip_ref:
            zip_ref.extractall(dir)
            zip_ref.extractall(f"{dir}/atual")
            if os.path.exists(f"{id}.zip"):
                os.remove(f"{id}.zip")
    except:
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
    client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(
        filename="logfile_soap_lattes_adm.log",
        filemode="w",
        format=Log_Format,
        level=logging.DEBUG,
    )
    logger = logging.getLogger()
    logger.debug("Inicio")

    dir = "/home/ejorge/hop/config/projects/Jade-Extrator-Hop/metadata/dataset/xml"

    for Files in os.listdir(dir):
        try:
            os.remove(os.path.join(dir, Files))
        except:
            logger.error("Erro 003: Directory")
    logger.debug("Arquivos XML removidos")

    quant_curriculos = 0

    df = get_researcher_adm_simcc()

    for Index, Data in df.iterrows():

        print(f"Curriculo número: {quant_curriculos}\n")
        print(f"ID do pesquisador: {Data['lattes_id']}\n")

        lattes_id = str(Data["lattes_id"]).zfill(16)
        save_cv(lattes_id, dir, os.getenv("CNPQ_SERVICE"))
        quant_curriculos += 1

    logger.debug(f"FIM: {str(quant_curriculos)}")
    print(f"FIM, Quantidade de curriculos processados: {str(quant_curriculos)}")
