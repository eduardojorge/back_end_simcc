from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile

from simcc.schemas.Conectee import ResearcherData
from simcc.services import ConecteeService

router = APIRouter()

STORAGE_PATH = Path('storage/conectee')
STORAGE_PATH.mkdir(parents=True, exist_ok=True)


@router.get(
    '/researcher',
    response_model=list[ResearcherData],
    status_code=HTTPStatus.OK,
)
def researcher(cpf: str = str(), name: str = str()):
    researcher = ConecteeService.get_researcher_data(cpf, name)
    return researcher


@router.post(
    '/researcher',
    status_code=HTTPStatus.CREATED,
)
def save_and_process_files(files: list[UploadFile]):
    successfull = []
    try:
        for file in files:
            file_path = STORAGE_PATH / file.filename
            with open(file_path, 'wb') as f:
                content = file.file.read()
                f.write(content)
            result = ConecteeService.extract_data(file_path)
            if result:
                successfull.append(file.filename)
            file_path.unlink()
        return {
            'message': 'Files processed and deleted successfully.',
            'successfull': successfull,
        }

    except Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(e),
        )
