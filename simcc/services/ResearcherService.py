from uuid import UUID

import pandas as pd
from numpy import nan

from simcc.repositories.simcc import ResearcherRepository
from simcc.schemas.Researcher import Researcher


def merge_researcher_data(researchers: pd.DataFrame) -> pd.DataFrame:
    sources = {
        'graduate_programs': ResearcherRepository.list_graduate_programs(),
        'research_groups': ResearcherRepository.list_research_groups(),
        'subsidy': ResearcherRepository.list_foment_data(),
        'departments': ResearcherRepository.list_departament_data(),
    }

    for column, source in sources.items():
        if source:
            dataframe = pd.DataFrame(source)
            researchers = researchers.merge(dataframe, on='id', how='left')
        else:
            researchers[column] = None

    ufmg_data = ResearcherRepository.list_ufmg_data()
    if ufmg_data:
        dataframe = pd.DataFrame(ufmg_data)
        researchers = researchers.merge(dataframe, on='id', how='left')
    else:
        columns = [
            'matric',
            'inscufmg',
            'genero',
            'situacao',
            'rt',
            'clas',
            'cargo',
            'classe',
            'ref',
            'titulacao',
            'entradanaufmg',
            'progressao',
            'semester',
        ]
        for column in columns:
            researchers[column] = None

    return researchers


def search_in_articles(
    terms: str = None,
    graduate_program_id: UUID = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_articles(
        terms, graduate_program_id, university, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def search_in_abstracts(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int = None,
    lenght: int = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_abstracts(
        terms, graduate_program_id, university, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def serch_in_name(
    name: str,
    graduate_program_id: UUID,
    dep_id: UUID,
    page: int,
    lenght: int,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_name(
        name, graduate_program_id, dep_id, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')
