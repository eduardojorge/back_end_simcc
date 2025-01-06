from uuid import UUID

import pandas as pd
from numpy import nan

from simcc.repositories.simcc import ResearcherRepository
from simcc.schemas.Researcher import Researcher


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

    programs = ResearcherRepository.list_graduate_programs()
    groups = ResearcherRepository.list_research_groups()
    foment_data = ResearcherRepository.list_foment_data()
    departaments = ResearcherRepository.list_departament_data()
    ufmg_data = ResearcherRepository.list_ufmg_data()

    researchers = pd.DataFrame(researchers)
    programs = pd.DataFrame(programs)
    groups = pd.DataFrame(groups)
    foment_data = pd.DataFrame(foment_data)
    departaments = pd.DataFrame(departaments)
    ufmg_data = pd.DataFrame(ufmg_data)

    researchers = researchers.merge(programs, on='id', how='left')
    researchers = researchers.merge(groups, on='id', how='left')
    researchers = researchers.merge(foment_data, on='id', how='left')
    researchers = researchers.merge(departaments, on='id', how='left')
    researchers = researchers.merge(ufmg_data, on='id', how='left')

    researchers = researchers.replace(nan, None)
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

    programs = ResearcherRepository.list_graduate_programs()
    groups = ResearcherRepository.list_research_groups()
    foment_data = ResearcherRepository.list_foment_data()
    departaments = ResearcherRepository.list_departament_data()
    ufmg_data = ResearcherRepository.list_ufmg_data()

    researchers = pd.DataFrame(researchers)
    programs = pd.DataFrame(programs)
    groups = pd.DataFrame(groups)
    foment_data = pd.DataFrame(foment_data)
    departaments = pd.DataFrame(departaments)
    ufmg_data = pd.DataFrame(ufmg_data)

    researchers = researchers.merge(programs, on='id', how='left')
    researchers = researchers.merge(groups, on='id', how='left')
    researchers = researchers.merge(foment_data, on='id', how='left')
    researchers = researchers.merge(departaments, on='id', how='left')
    researchers = researchers.merge(ufmg_data, on='id', how='left')

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

    programs = ResearcherRepository.list_graduate_programs()
    groups = ResearcherRepository.list_research_groups()
    foment_data = ResearcherRepository.list_foment_data()
    departaments = ResearcherRepository.list_departament_data()
    ufmg_data = ResearcherRepository.list_ufmg_data()

    researchers = pd.DataFrame(researchers)
    programs = pd.DataFrame(programs)
    groups = pd.DataFrame(groups)
    foment_data = pd.DataFrame(foment_data)
    departaments = pd.DataFrame(departaments)
    ufmg_data = pd.DataFrame(ufmg_data)

    researchers = researchers.merge(programs, on='id', how='left')
    researchers = researchers.merge(groups, on='id', how='left')
    researchers = researchers.merge(foment_data, on='id', how='left')
    researchers = researchers.merge(departaments, on='id', how='left')
    researchers = researchers.merge(ufmg_data, on='id', how='left')

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')
