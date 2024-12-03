from uuid import UUID

from ..dao import Connection
from ..model.article import Article, Qualis


def article_search(
    researcher_id: UUID, qualis: Qualis, year=int, terms=str
) -> list[Article]:
    params = {}
    filters = []
    if researcher_id:
        params['researcher_id'] = researcher_id
        filters.append('AND researcher_id = %(researcher_id)s')

    if qualis:
        params['qualis'] = qualis.split(';')
        filters.append('AND UNACCENT(LOWER(qualis)) = ANY(%(qualis)s)')

    if terms:
        params['terms'] = build_search_query(terms)
        filters.append(r"AND (ts_rank(to_tsvector(translate(unaccent(LOWER(title)),'-\.:;''',' ')), websearch_to_tsquery(%(terms)s)) > 0.04)")  # noqa: E501 # fmt: skip

    if year:
        params['year'] = year
        filters.append('AND b.year::int >= %(year)s')

    SCRIPT_SQL = f"""
        SELECT DISTINCT
            b.id AS id,
            title,
            b.year AS year,
            type,
            doi,
            ba.qualis,
            periodical_magazine_name AS magazine,
            r.name AS researcher,
            r.lattes_10_id AS lattes_10_id,
            r.lattes_id AS lattes_id,
            jcr AS jif,
            jcr_link,
            r.id as researcher_id
        FROM
            bibliographic_production b
            LEFT JOIN bibliographic_production_article ba
            ON b.id = ba.bibliographic_production_id
            LEFT JOIN researcher r ON r.id = b.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        WHERE
            b.type = 'ARTICLE'
            {'\n'.join(filters) if filters else ''}
        ORDER BY
            year DESC;
        """

    with Connection() as conn:
        researcher_list = conn.select(SCRIPT_SQL, params)
    return researcher_list


def build_search_query(search_string):
    operators = {'&', '!', '|', '(', ')'}

    search_string = search_string.replace(';', ' & ')
    search_string = search_string.replace('.', ' ! ')
    search_string = search_string.replace('|', ' | ')

    terms = search_string.split()
    for i, term in enumerate(terms):
        if term not in operators:
            terms[i] = f'"{term}"'

    search_query = ' '.join(terms)

    return search_query
