from uuid import UUID

import pandas as pd
from numpy import nan

from simcc.repositories.simcc import ResearcherRepository
from simcc.schemas.Researcher import CoAuthorship, Researcher


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


def opa_co_authorship(researcher_id: UUID) -> list:
    co_authorship = ResearcherRepository.list_openalex_co_authorship(
        researcher_id
    )
    if not co_authorship:
        return []

    co_authorship = pd.DataFrame(co_authorship)

    co_authorship = co_authorship.groupby('name').agg(
        among=('name', 'size'),
        institution=('institution', lambda x: x.tolist()),
    )
    co_authorship = co_authorship.reset_index()

    co_authorship['institution_id'] = co_authorship['institution'].apply(
        ResearcherRepository.get_institutions
    )

    def get_id(name: str) -> UUID:
        researcher_id = ResearcherRepository.get_id(name)
        if researcher_id:
            return researcher_id.get('id')
        return None

    co_authorship['id'] = co_authorship['name'].apply(get_id)

    return co_authorship.to_dict(orient='records')


def list_co_authorship(researcher_id: UUID) -> list[CoAuthorship]:
    co_authorship = ResearcherRepository.list_co_authorship(researcher_id)
    co_authorship += opa_co_authorship(researcher_id)

    if not co_authorship:
        return []

    institution_id = ResearcherRepository.get_institution_id(researcher_id)
    institution_id = institution_id.get('institution_id', None)

    co_authorship = pd.DataFrame(co_authorship)

    def co_authorship_type(co_authorship_institution):
        if co_authorship_institution == institution_id:
            return 'internal'
        return 'external'

    co_authorship['type'] = co_authorship['institution_id'].apply(
        co_authorship_type
    )
    return co_authorship.to_dict(orient='records')
