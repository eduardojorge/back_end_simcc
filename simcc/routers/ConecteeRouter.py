from http import HTTPStatus

from fastapi import APIRouter

from simcc.schemas.Conectee import ResearcherData
from simcc.services import ConecteeService

router = APIRouter()


@router.get(
    '/researcher',
    response_model=list[ResearcherData],
    status_code=HTTPStatus.OK,
)
def researcher(cpf: str = str(), name: str = str()):
    researcher = ConecteeService.get_researcher_data(cpf, name)
    return researcher
