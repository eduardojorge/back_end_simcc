from zeep import Client
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import zipfile
import pandas as pd

import os
import sys
import logging
from datetime import datetime
import Dao.sgbdSQL as db
import project as project


def get_data_att(id: str) -> datetime:
    resultado = client.service.getDataAtualizacaoCV(id)
    if resultado != None:
        return datetime.strptime(resultado, "%d/%m/%Y %H:%M:%S")


def last_update(xml_filename):
    tree = ET.parse(f"{dir}/atual/{xml_filename}")
    root = tree.getroot()
    lista = [
        i
        for i in root.items()
        if i[0] == "DATA-ATUALIZACAO" or i[0] == "HORA-ATUALIZACAO"
    ]
    return datetime.strptime(lista[0][1] + lista[1][1], "%d%m%Y%H%M%S")


def get_id_cnpq(name: str = str(), date: str = str(), CPF: str = str()):
    resultado = client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )
    if resultado != None:
        return resultado


def salvarCV(id, dir):
    try:
        data = get_data_att(id)
        if data <= last_update(f"{id}.xml"):
            print("Currículo já está atualizado id:" + str(id))
            logger.debug("Currículo já está atualizado id:" + str(id))
            return
    except:
        print("\nCurrículo não  atualizado id:" + str(id))
        logger.debug("\nCurrículo não  atualizado id:" + str(id))

    try:
        resultado = client.service.getCurriculoCompactado(id)
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
    project.project_env = "8"

    script_sql = """
        SELECT 
            name,
            lattes_id
        FROM
            researcher;
        """
    registry = db.consultar_db(script_sql)

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

        print(
            f"Curriculo número: {quant_curriculos}\nID do pesquisador: {Data['lattes_id']}"
        )

        lattes_id = str(Data["lattes_id"])

        if len(str(Data["lattes_id"])) == 14:
            lattes_id = "00" + str(Data["lattes_id"])
        elif len(str(Data["lattes_id"])) == 15:
            lattes_id = "0" + str(Data["lattes_id"])

        salvarCV(
            lattes_id,
            dir,
        )
        quant_curriculos += 1

    logger.debug(f"FIM: {str(quant_curriculos)}")
    print(f"FIM, Quantidade de curriculos processados: {str(quant_curriculos)}")
