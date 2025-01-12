from simcc.repositories import ConecteeRepository
from simcc.schemas.Conectee import ResearcherData


def get_researcher_data(cpf: str, name: str) -> list[ResearcherData]:
    researcher = ConecteeRepository.get_researcher(cpf, name)
    if not researcher:
        return []
    return researcher
