from collections import Counter
from uuid import UUID

import pandas as pd

from simcc.repositories.simcc import ProductionRepository
from simcc.schemas.Production.Article import ArticleMetric
from simcc.schemas.Production.Book import BookProduction
from simcc.schemas.Production.Brand import BrandProduction
from simcc.schemas.Production.Guidance import GuidanceMetrics
from simcc.schemas.Production.Patent import PatentMetric, PatentProduction
from simcc.schemas.Researcher import AcademicMetric


def list_article_metrics(
    researcher_id: UUID,
    program_id: UUID,
    year: int,
) -> list[ArticleMetric]:
    article_metrics = ProductionRepository.list_article_metrics(
        researcher_id, program_id, year
    )
    if not article_metrics:
        return []

    def count_qualis(qualis):
        return dict(Counter(qualis))

    def count_jcr(jcr):
        bins = [0.0, 0.65, 2.0, 4.0, float('inf')]
        labels = ['very_low', 'low', 'medium', 'high']
        jcr_metrics = pd.cut(
            pd.to_numeric(jcr, errors='coerce'),
            bins=bins,
            labels=labels,
        )
        jcr_metrics = jcr_metrics.value_counts()
        jcr_metrics = jcr_metrics.to_dict()

        jcr_metrics['not_applicable'] = jcr.count('N/A')
        jcr_metrics['without_jcr'] = jcr.count(None)
        return jcr_metrics

    article_metrics = pd.DataFrame(article_metrics)
    article_metrics['qualis'] = article_metrics['qualis'].apply(count_qualis)
    article_metrics['jcr'] = article_metrics['jcr'].apply(count_jcr)
    article_metrics['citations'] = article_metrics['citations'].fillna(0)
    return article_metrics.to_dict(orient='records')


def list_patent_metrics(researcher_id: UUID, year: int) -> list[PatentMetric]:
    patent_metrics = ProductionRepository.list_patent_metrics(
        researcher_id, year
    )
    if not patent_metrics:
        return []
    return patent_metrics


def list_guidance_metrics(
    researcher_id: UUID, year: int
) -> list[GuidanceMetrics]:
    guidance_metrics = ProductionRepository.list_guidance_metrics(
        researcher_id, year
    )
    if not guidance_metrics:
        return []
    guidance_metrics = pd.DataFrame(guidance_metrics)

    guidance_metrics = guidance_metrics.pivot_table(
        index='year',
        columns='nature',
        values='count_nature',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    rename_dict = {
        'iniciacao cientifica concluida': 'ic_completed',
        'iniciacao cientifica em andamento': 'ic_in_progress',
        'dissertacao de mestrado concluida': 'm_completed',
        'dissertacao de mestrado em andamento': 'm_in_progress',
        'tese de doutorado concluida': 'd_completed',
        'tese de doutorado em andamento': 'd_in_progress',
        'trabalho de conclusao de curso graduacao concluida': 'g_completed',
        'trabalho de conclusao de curso de graduacao em andamento': 'g_in_progress',
        'monografia de conclusao de curso aperfeicoamento e especializacao concluida': 'e_completed',
        'monografia de conclusao de curso aperfeicoamento e especializacao em andamento': 'e_in_progress',
        'orientacao-de-outra-natureza concluida': 'o_completed',
        'supervisao de pos-doutorado concluida': 'sd_completed',
        'supervisao de pos-doutorado em andamento': 'sd_in_progress',
    }

    guidance_metrics.rename(columns=rename_dict, inplace=True)

    return guidance_metrics.to_dict(orient='records')


def list_academic_degree_metrics(
    researcher_id: UUID, year: int
) -> list[AcademicMetric]:
    degree_metrics = ProductionRepository.list_academic_degree_metrics(
        researcher_id, year
    )
    if not degree_metrics:
        return []
    degree_metrics = pd.DataFrame(degree_metrics)
    degree_metrics['degree'] = degree_metrics['degree'].str.lower()
    degree_metrics = degree_metrics.pivot_table(
        index='year',
        columns='degree',
        values='among',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    columns = AcademicMetric.model_fields.keys()
    degree_metrics = degree_metrics.reindex(
        columns, axis='columns', fill_value=0
    )

    return degree_metrics.to_dict(orient='records')


def list_software_metrics(researcher_id: UUID, year: int):
    metrics = ProductionRepository.list_software_metrics(researcher_id, year)
    return metrics


def list_distinct_patent(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
) -> list[PatentProduction]:
    patents = ProductionRepository.list_distinct_patent(
        term, researcher_id, year, page, lenght
    )
    if not patents:
        return []
    return patents


def list_patent(
    term: str, researcher_id: UUID, year: int, page: int, lenght: int
) -> list[PatentProduction]:
    patents = ProductionRepository.list_patent(
        term, researcher_id, year, page, lenght
    )
    if not patents:
        return []
    return patents


def list_brand(
    researcher_id: UUID, year: int, page: int, lenght: int
) -> list[BrandProduction]:
    brands = ProductionRepository.list_brand(researcher_id, year, page, lenght)
    if not brands:
        return []
    return brands


def list_distinct_book(
    researcher_id: UUID, year: int, page: int, lenght: int
) -> list[BookProduction]:
    books = ProductionRepository.list_distinct_book(
        researcher_id, year, page, lenght
    )
    if not books:
        return []
    return books


def list_book(researcher_id: UUID, year: int, page: int, lenght: int):
    patents = ProductionRepository.list_book(researcher_id, year, page, lenght)
    if not patents:
        return []
    return patents
