from uuid import UUID

from simcc.repositories.simcc import ProductionRepository


def list_article_metrics(program_id: UUID, year: int):
    article_metrics = ProductionRepository.list_article_metrics()
    return article_metrics
