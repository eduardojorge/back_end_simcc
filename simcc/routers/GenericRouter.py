import os
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from zeep import Client

from simcc.schemas import ResearcherBarema, YearBarema
from simcc.services import GenericService

STORAGE_PATH = Path('storage/dictionary')
STORAGE_PATH.mkdir(parents=True, exist_ok=True)

router = APIRouter()


@router.get('/logs_researcher')
def get_researcher_logs(): ...


@router.get('/logs')
def get_logs():
    return GenericService.get_logs()


@router.get('/foment')
def get_researcher_foment(institution_id: UUID = None):
    return GenericService.get_researcher_foment(institution_id)


@router.get('/dictionary.pdf')
def dim_titulacao_xlsx():
    file_path = os.path.join(STORAGE_PATH, 'dictionary.pdf')
    return FileResponse(file_path, filename='dictionary.pdf')


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
    response_model=str,
    status_code=HTTPStatus.OK,
)
def current_lattes_date(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    response = client.service.getDataAtualizacaoCV(lattes_id)
    if response:
        return datetime.strptime(response, '%d/%m/%Y %H:%M:%S').strftime(
            '%d/%m/%Y %H:%M:%S'
        )
    raise HTTPException(status_code=404, detail='Curriculum not found')


@router.get(
    '/resarcher_barema',
    status_code=HTTPStatus.OK,
    response_model=list[ResearcherBarema],
)
def resarcher_barema(
    name: Optional[str] = Query(None),
    lattes_id: Optional[str] = Query(None),
    yarticle: Optional[str] = Query(None),
    ywork_event: Optional[str] = Query(None),
    ybook: Optional[str] = Query(None),
    ychapter_book: Optional[str] = Query(None),
    ypatent: Optional[str] = Query(None),
    ysoftware: Optional[str] = Query(None),
    ybrand: Optional[str] = Query(None),
    yresource_progress: Optional[str] = Query(None),
    yresource_completed: Optional[str] = Query(None),
    yparticipation_events: Optional[str] = Query(None),
):
    year = YearBarema(
        article=yarticle,
        work_event=ywork_event,
        book=ybook,
        chapter_book=ychapter_book,
        patent=ypatent,
        software=ysoftware,
        brand=ybrand,
        resource_progress=yresource_progress,
        resource_completed=yresource_completed,
        participation_events=yparticipation_events,
    )

    return GenericService.barema_production(name, lattes_id, year)
