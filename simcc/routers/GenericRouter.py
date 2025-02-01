from datetime import datetime
from http import HTTPStatus

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from zeep import Client

router = APIRouter()


@router.get(
    '/getCurriculoCompactado',
    response_class=FileResponse,
    status_code=HTTPStatus.OK,
)
def lattes_xml(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    response = client.service.getCurriculoCompactado(lattes_id)
    if response:
        file_path = f'storage/{lattes_id}.zip'
        with open(file_path, 'wb') as file:
            file.write(response)
        return FileResponse(
            path=file_path,
            filename=f'{lattes_id}.zip',
            media_type='application/zip',
        )
    raise HTTPException(status_code=404, detail='Curriculum not found')


@router.get(
    '/getDataAtualizacaoCV',
    response_model=datetime,
    status_code=HTTPStatus.OK,
)
def current_lattes_date(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    response = client.service.getDataAtualizacaoCV(lattes_id)
    if response:
        response = datetime.strptime(response, '%d/%m/%Y %H:%M:%S')
        return response.strftime('%d/%m/%Y %H:%M:%S')
    raise HTTPException(status_code=404, detail='Curriculum not found')
