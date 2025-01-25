from fastapi import APIRouter
from zeep import Client

router = APIRouter()


@router.get('/getCurriculoCompactado')
def getCurriculoCompactado(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    result = client.service.getCurriculoCompactado(lattes_id)
    file = open(f'storage/{lattes_id}.zip', 'wb')
    file.write(result)
    file.close()
