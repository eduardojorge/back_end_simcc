import logging
import os
import zipfile
from datetime import datetime

import httpx
from zeep import Client

from simcc.config import settings
from simcc.repositories import conn, conn_admin

client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')

LOG_PATH = 'logs'
CURRENT_XML_PATH = 'current'
ZIP_XML_PATH = 'zip'

PROXY = settings.ALTERNATIVE_CNPQ_SERVICE


def list_admin_researchers():
    SCRIPT_SQL = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher;
        """
    result = conn_admin.select(SCRIPT_SQL)
    return result


def cnpq_att(lattes_id) -> datetime:
    if PROXY:
        PROXY_URL = f'https://simcc.uesc.br/api/getDataAtualizacaoCV?lattes_id={lattes_id}'
        if response := httpx.get(PROXY_URL, verify=False).json():
            return datetime.strptime(response, '%d/%m/%Y %H:%M:%S')
        return datetime.min

    response = client.service.getDataAtualizacaoCV(id)
    return datetime.strptime(response, '%d/%m/%Y %H:%M:%S')


def database_att(lattes_id) -> datetime:
    params = {'lattes_id': lattes_id}
    SCRIPT_SQL = """
        SELECT last_update
        FROM researcher
        WHERE lattes_id = %(lattes_id)s;
        """
    result = conn.select(SCRIPT_SQL, params)
    if result:
        return result[0].get('last_update')
    return datetime.min


def download_xml(lattes_id):
    if cnpq_att(lattes_id) <= database_att(lattes_id):
        print('Curriculo atualizado!')
        return

    print('Baixando curriculo...')

    SCRIPT_SQL = """
        UPDATE researcher
        SET updated = false
        WHERE lattes_id = %(lattes_id)s;
        """
    conn.exec(SCRIPT_SQL, {'lattes_id': lattes_id})

    if PROXY:
        PROXY_URL = f'https://simcc.uesc.br/api/getCurriculoCompactado?lattes_id={lattes_id}'
        response = httpx.get(PROXY_URL, verify=False).content
    else:
        response = client.service.getCurriculoCompactado(lattes_id)

    zip_path = os.path.join(HOP_PATH, ZIP_XML_PATH, lattes_id + '.zip')
    with open(zip_path, 'wb') as XML:
        XML.write(response)

    with zipfile.ZipFile(zip_path, 'r') as ZIP:
        ZIP.extractall(HOP_PATH)
        ZIP.extractall(os.path.join(HOP_PATH, CURRENT_XML_PATH))
    os.remove(zip_path)


if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'soap.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    HOP_PATH = 'config/projects/Jade-Extrator-Hop/metadata/dataset/xml/'
    HOP_PATH = os.path.join(settings.JADE_EXTRATOR_FOLTER, HOP_PATH)

    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    for file in os.listdir(HOP_PATH):
        file_path = os.path.join(HOP_PATH, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    researchers = list_admin_researchers()
    for _, researcher in enumerate(researchers):
        print(f'Curriculo número: [{_}]')
        print(f'Pesquisador: [{researcher.get("name")}]')
        print(f'Lattes: [{researcher.get("lattes_id")}]')

        lattes_id = researcher.get('lattes_id')
        lattes_id = lattes_id.zfill(16)
        download_xml(lattes_id)
    print('FIM!')
