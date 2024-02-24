from zeep import Client
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import zipfile
import pandas as pd

import logging
from datetime import datetime
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
    CPF = extract_int(CPF)
    resultado = client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )
    if resultado:
        return resultado.zfill(16)
    else:
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


def extract_int(string: str) -> str:
    sanitized_string = str()

    for character in string:
        if character.isdigit():
            sanitized_string += character

    return sanitized_string


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

    quant_loss = 0

    df = pd.read_excel("Files/researcher_ufsb.xlsx")

    for Index, Data in df.iterrows():
        print(f"Loading: {Index}")
        lattes_id = get_id_cnpq(CPF=extract_int(str(Data["CPF"])))

        if lattes_id:
            salvarCV(
                lattes_id,
                dir,
            )
        else:
            quant_loss += 1

    logger.debug(f"Download concluido, quantidade de perdas: {str(quant_loss)}")
    print(f"FIM, Quantidade de curriculos processados: {str(quant_loss)}")
