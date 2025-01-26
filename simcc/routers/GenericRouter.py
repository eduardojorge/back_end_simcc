from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from zeep import Client

router = APIRouter()


@router.get('/getCurriculoCompactado')
def getCurriculoCompactado(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    result = client.service.getCurriculoCompactado(lattes_id)

    if result:
        file_path = f'storage/{lattes_id}.zip'

        with open(file_path, 'wb') as file:
            file.write(result)

        return FileResponse(
            path=file_path,
            filename=f'{lattes_id}.zip',
            media_type='application/zip',
        )

    raise HTTPException(status_code=404, detail='Curriculum not found')
