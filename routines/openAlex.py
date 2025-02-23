import json
import time

import pandas as pd
import requests

from routines.logger import logger_researcher_routine, logger_routine
from simcc.repositories import conn


def scrapping_article_data():
    OPENALEX_URL = 'https://api.openalex.org/works/https://doi.org/'

    SCRIPT_SQL = """
        SELECT bp.doi, bp.id
        FROM public.bibliographic_production bp
        LEFT OUTER JOIN openalex_article opa ON bp.id = opa.article_id
        WHERE doi IS NOT NULL AND opa.article_id IS NULL;
        """

    result = conn.select(SCRIPT_SQL)
    dataframe = pd.DataFrame(result)

    for _, data in dataframe.iterrows():
        ARTICLE_URL = OPENALEX_URL + data['doi']
        print(ARTICLE_URL)
        ATICLE_FILE = f'storage/openAlex/article/{data["id"]}.json'
        response = requests.get(ARTICLE_URL)

        if response.status_code == 200:
            with open(ATICLE_FILE, 'w', encoding='utf-8') as buffer:
                json.dump(response.json(), buffer)
                try:
                    extract_article(data['id'], response.json())
                except Exception as e:
                    print(f'[ERROR] - {e}')
            print('[201] - CREATED ARTICLE')
        else:
            with open(ATICLE_FILE, 'w', encoding='utf-8') as buffer:
                response = str(response.status_code)
                buffer.write(response)
            print('[404] - NOT FOUND ARTICLE')
        time.sleep(6)


def scrapping_researcher_data():
    OPENALEX_URL = 'https://api.openalex.org/authors/orcid:'

    SCRIPT_SQL = """
        SELECT r.orcid, r.id
        FROM researcher r
        LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE r.orcid IS NOT NULL AND opr.researcher_id IS NULL;
        """

    result = conn.select(SCRIPT_SQL)
    dataframe = pd.DataFrame(result)

    for _, data in dataframe.iterrows():
        RESEARCHER_URL = OPENALEX_URL + data['orcid']
        RESEARCHER_FILE = f'storage/openAlex/researcher/{data["id"]}.json'
        response = requests.get(RESEARCHER_URL)

        if response.status_code == 200:
            with open(RESEARCHER_FILE, 'w', encoding='utf-8') as buffer:
                json.dump(response.json(), buffer)
                try:
                    extract_researcher(data['id'], response.json())
                except Exception as e:
                    print(f'[ERROR] - {e}')
            print('[201] - CREATED RESEARCHER')
        else:
            with open(RESEARCHER_FILE, 'w', encoding='utf-8') as buffer:
                response = str(response.status_code)
                buffer.write(response)
            print('[404] - NOT FOUND RESEARCHER')
        time.sleep(6)


def extract_article(id, data):  # noqa: PLR0914
    if ISSN := data.get('primary_location', ''):
        if ISSN := ISSN.get('source', ''):
            ISSN = ISSN.get('issn')

    if ARTICLE_INSTITUTION := data.get('primary_location', ''):
        if ARTICLE_INSTITUTION := ARTICLE_INSTITUTION.get('source', ''):
            ARTICLE_INSTITUTION = ARTICLE_INSTITUTION.get('display_name')

    if ABSTRACT := data.get('abstract_inverted_index', None):
        length = max(max(index) for index in ABSTRACT.values())

        inverted_abstract = list(range(length + 1))

        for word in ABSTRACT.items():
            for index in word[1]:
                inverted_abstract[index] = word[0]
        ABSTRACT = ' '.join(inverted_abstract)

    AUTHORSHIPS = data.get('authorships')

    AUTHORS_LIST = []
    AUTHORS_INSTITUTION_LIST = []

    for author in AUTHORSHIPS:
        author = author.get('author', {})
        author = author.get('display_name', {})
        AUTHORS_LIST.append(author)

    for authors_institution in AUTHORSHIPS:
        authors_institution = authors_institution.get('institutions')
        xpto = []
        for institution in authors_institution:
            xpto.append(institution.get('display_name'))
        AUTHORS_INSTITUTION_LIST.append(', '.join(xpto))

    AUTHORS_INSTITUTION_LIST = '; '.join(AUTHORS_INSTITUTION_LIST)
    AUTHORS_LIST = '; '.join(AUTHORS_LIST)

    LANGUAGE = data.get('language')
    CITATIONS = sum(
        count.get('cited_by_count') for count in data.get('counts_by_year')
    )

    if DOWNLOAD_LINK := data.get('primary_location', ''):
        DOWNLOAD_LINK = data.get('pdf_url')

    if LANDING_PAGE_URL := data.get('primary_location', ''):
        LANDING_PAGE_URL = data.get('landing_page_url')

    KEYWORDS = [word.get('display_name') for word in data.get('keywords')]
    KEYWORDS = '; '.join(KEYWORDS)

    xpto = [id, ARTICLE_INSTITUTION, ISSN, ABSTRACT, AUTHORS_LIST, AUTHORS_INSTITUTION_LIST, LANGUAGE, CITATIONS, DOWNLOAD_LINK, LANDING_PAGE_URL, KEYWORDS]  # fmt: skip  # noqa: E501

    SCRIPT_SQL = """
        INSERT INTO public.openalex_article
            (article_id, article_institution, issn, abstract, authors,
            authors_institution, language, citations_count, pdf, landing_page_url,
            keywords)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

    conn.exec(SCRIPT_SQL, xpto)
    print(f'Insert concluido! [{id}]')


def extract_researcher(id, data):
    if H_INDEX := data.get('summary_stats', None):
        H_INDEX = H_INDEX.get('h_index')

    if I10_INDEX := data.get('summary_stats', None):
        I10_INDEX = I10_INDEX.get('i10_index')

    if ORCID := data.get('ids', ''):
        if ORCID := ORCID.get('orcid', ''):
            ORCID = ORCID[-18:]

    if SCOPUS := data.get('ids', None):
        SCOPUS = SCOPUS.get('scopus')

    OPEN_ALEX = data.get('id')

    WORKS_COUNT = data.get('works_count')

    CITED_BY_COUNT = data.get('cited_by_count')

    xpto = [id, H_INDEX, 0, WORKS_COUNT, CITED_BY_COUNT, I10_INDEX, SCOPUS, ORCID, OPEN_ALEX]  # fmt: skip

    SCRIPT_SQL = """
        INSERT INTO public.openalex_researcher
        (researcher_id, h_index, relevance_score, works_count, cited_by_count, i10_index, scopus, orcid, openalex)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

    conn.exec(SCRIPT_SQL, xpto)
    logger_researcher_routine(id, 'OPEN_ALEX', False)
    print(f'Insert concluido! [{id}]')


if __name__ == '__main__':
    scrapping_researcher_data()
    scrapping_article_data()
    logger_routine('OPEN_ALEX', False)
