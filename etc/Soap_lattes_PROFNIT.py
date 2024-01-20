from zeep import Client
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import zipfile
import pandas as pd

import os
import logging
from datetime import datetime


client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")
# client.transport.session.proxies = {'http': # Proxy da UNEB ,
#                                    'https':  #Proxy da UNEB}


def get_DataAtualização(id: str) -> datetime:
    # Retorna a data de atualização do CV

    resultado = client.service.getDataAtualizacaoCV(id)

    if resultado != None:
        return datetime.strptime(resultado, "%d/%m/%Y %H:%M:%S")


def last_update(xml_filename):
    tree = ET.parse(dir + "atual/" + xml_filename)
    root = tree.getroot()
    lista = [
        i
        for i in root.items()
        if i[0] == "DATA-ATUALIZACAO" or i[0] == "HORA-ATUALIZACAO"
    ]
    return datetime.strptime(lista[0][1] + lista[1][1], "%d%m%Y%H%M%S")


def getIdentificadorCNPQ(nome, data):
    # Retorna o identificador do CNPQ
    resultado = client.service.getIdentificadorCNPq(
        nomeCompleto=nome, dataNascimento=data, cpf=""
    )
    if resultado != None:
        return resultado


def salvarCV(id, dir):
    data = get_DataAtualização(id)
    print(data)
    try:
        if data <= last_update(id + ".xml"):
            # print('Currículo já está atualizado')
            logger.debug("Currículo já está atualizado id:" + str(id))
            return
    except:
        # print('Currículo não  atualizado id:'+id)
        logger.debug("Currículo não  atualizado id:" + str(id))

    print(id)
    try:
        resultado = client.service.getCurriculoCompactado(id)
        arquivo = open(dir + "/zip/" + id + ".zip", "wb")
        arquivo.write(resultado)
        arquivo.close()

        with zipfile.ZipFile(dir + "/zip/" + id + ".zip", "r") as zip_ref:
            zip_ref.extractall(dir)
            zip_ref.extractall(dir + "/atual")
            if os.path.exists(id + ".zip"):
                os.remove(id + ".zip")
    except:
        #    print('----------Err:Currículo não  existe')
        logger.error("----------Err:Currículo não  existe")


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename="logfile_SOAP_Lattes_profnit.log",
    filemode="w",
    format=Log_Format,
    level=logging.DEBUG,
)

logger = logging.getLogger()

# Testing our Logger


logger.debug("Inicio")

dir = "/home/eduardomfjorge/curriculos/"
# dir='/home/pendulum/curriculos/'

for f in os.listdir(dir):
    try:
        os.remove(os.path.join(dir, f))
    except:
        logger.error("Dir")

logger.debug("Arquivos XML removidos")

# df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
df = pd.read_excel(r"files/PesquisadoresProfnit.xlsx")
print(df)
LATTES_ID = 0

x = 0
for i, infos in df.iterrows():
    print("teste x " + str(infos[LATTES_ID]))
    print(len(str(infos[LATTES_ID])))
    if len(str(infos[LATTES_ID])) == 14:
        lattes_id = "00" + str(infos[LATTES_ID])
    elif len(str(infos[LATTES_ID])) == 15:
        lattes_id = "0" + str(infos[LATTES_ID])
    else:
        lattes_id = str(infos[LATTES_ID])

    salvarCV(lattes_id, dir)
    x = x + 1
print("Fim " + str(x))
logger.debug("Fim Total:" + str(x))

print("------------Registro:" + str(x))
print("------------Fim " + str(x))
# salvarCV('5674134492786099','/home/eduardomfjorge/teste/curriculos')
