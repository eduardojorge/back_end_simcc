from uuid import UUID

import pandas as pd

from simcc.repositories import GraduateProgramRepository, ResearcherRepository
from simcc.schemas.GraduateProgram import GraduateProgram
from simcc.schemas.Researcher import ResearcherArticleProduction


def list_graduate_programs() -> GraduateProgram:
    graduate_programs = GraduateProgramRepository.select_graduate_program()
    return graduate_programs


def list_article_production(
    program_id: UUID, year: int
) -> ResearcherArticleProduction:
    article_production = ResearcherRepository.list_article_production(
        program_id, year
    )
    if not article_production:
        return []

    article_production = pd.DataFrame(article_production)

    article_production_pivot = article_production.pivot_table(
        index=['name', 'year'], columns='qualis', aggfunc='size', fill_value=0
    )

    citations = (
        article_production.groupby(['name', 'year'])['citations']
        .sum()
        .reset_index()
    )

    article_production_pivot = article_production_pivot.merge(
        citations, on=['name', 'year'], how='left'
    )
    columns = ['name', 'year', 'A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'BC', 'citations']  # fmt: skip  # noqa: E501
    article_production = article_production_pivot.reindex(
        columns, axis='columns', fill_value=0
    )

    return article_production.to_dict(orient='records')
