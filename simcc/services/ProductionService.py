from collections import Counter
from uuid import UUID

import pandas as pd

from simcc.repositories.simcc import ProductionRepository
from simcc.schemas.Article import ArticleMetric


def list_article_metrics(program_id: UUID, year: int) -> list[ArticleMetric]:
    article_metrics = ProductionRepository.list_article_metrics(program_id, year)
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
