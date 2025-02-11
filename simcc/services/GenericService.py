import pandas as pd
from numpy import nan

from routines.researcher_class import (
    article_metrics,
    book_chapter_metrics,
    book_metrics,
    brand_metrics,
    event_organization_metrics,
    guidance_metrics,
    participation_events_metrics,
    patent_metrics,
    software_metrics,
    work_in_event_metrics,
)
from simcc.repositories.simcc import GenericRepository
from simcc.schemas import ResearcherBarema, YearBarema


def barema_production(
    name: str, lattes_id: str, year: YearBarema
) -> list[ResearcherBarema]:
    lattes_list = []
    if lattes_id:
        lattes_list = lattes_id.split(';')
        researchers = GenericRepository.lattes_list(lattes=lattes_list)
    elif name.upper() == 'TODOS':
        researchers = GenericRepository.lattes_list()
    else:
        names = name.split(';')
        researchers = GenericRepository.lattes_list(names=names)

    researchers = pd.DataFrame(researchers)

    articles = article_metrics(year.article)
    articles = pd.DataFrame(articles)
    researchers = researchers.merge(articles, on='researcher_id', how='left')

    softwares = software_metrics(year.software)
    softwares = pd.DataFrame(softwares)
    researchers = researchers.merge(softwares, on='researcher_id', how='left')

    brands = brand_metrics(year.brand)
    brands = pd.DataFrame(brands)
    researchers = researchers.merge(brands, on='researcher_id', how='left')

    books = book_metrics(year.book)
    books = pd.DataFrame(books)
    researchers = researchers.merge(books, on='researcher_id', how='left')

    book_chapters = book_chapter_metrics(year.chapter_book)
    book_chapters = pd.DataFrame(book_chapters)
    researchers = researchers.merge(
        book_chapters, on='researcher_id', how='left'
    )

    patents = patent_metrics(year.patent)
    patents = pd.DataFrame(patents)
    p_ngranted = 'patent_not_granted'
    p_granted = 'patent_granted'
    patents = patents[[p_ngranted, p_granted, 'researcher_id']]
    patents['patent'] = patents[p_ngranted] + patents[p_granted]
    researchers = researchers.merge(patents, on='researcher_id', how='left')

    guidances = guidance_metrics(year.resource_completed)
    guidances = pd.DataFrame(guidances)
    researchers = researchers.merge(guidances, on='researcher_id', how='left')

    events_organizations = event_organization_metrics(year.participation_events)
    events_organizations = pd.DataFrame(events_organizations)
    researchers = researchers.merge(
        events_organizations, on='researcher_id', how='left'
    )

    work_in_event = work_in_event_metrics(year.work_event)
    work_in_event = pd.DataFrame(work_in_event)
    researchers = researchers.merge(
        work_in_event, on='researcher_id', how='left'
    )

    participation_events = participation_events_metrics(
        year.participation_events
    )
    participation_events = pd.DataFrame(participation_events)
    researchers = researchers.merge(
        participation_events, on='researcher_id', how='left'
    )

    researchers = researchers.replace(nan, 0)
    return researchers.to_dict(orient='records')
