import datetime
from uuid import uuid4

import pandas as pd

from simcc.repositories import conn


def article_metrics(year):
    SCRIPT_SQL = """
        SELECT qualis, COUNT(*) AS count_article, researcher_id
        FROM public.bibliographic_production bp
            JOIN public.bibliographic_production_article bpa ON
                bp.id = bpa.bibliographic_production_id
        WHERE type = 'ARTICLE' AND year_ >= %(year)s
        GROUP BY qualis, researcher_id;
        """

    result = conn.select(SCRIPT_SQL, {'year': year})
    articles = pd.DataFrame(result)

    articles = articles.pivot_table(
        index=['researcher_id'],
        columns='qualis',
        aggfunc='sum',
        fill_value=0,
    )

    articles.columns = articles.columns.get_level_values(1)
    articles = articles.reset_index()

    columns = [
        'researcher_id',
        'A1',
        'A2',
        'A3',
        'A4',
        'B1',
        'B2',
        'B3',
        'B4',
        'C',
        'SQ',
    ]
    articles = articles.reindex(columns, axis='columns', fill_value=0)
    articles = articles.to_dict(orient='records')
    if articles:
        return articles
    return [{'researcher_id': uuid4, 'A1': 0, 'A2': 0, 'A3': 0, 'A4': 0, 'B1': 0, 'B2': 0, 'B3': 0, 'B4': 0, 'C': 0, 'SQ': 0}]  # noqa: E501 # fmt: skip


def patent_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id,
            COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS PATENT_NOT_GRANTED,
            COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS PATENT_GRANTED
        FROM patent p
        WHERE development_year::INT >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    if result:
        return result
    return [{'researcher_id': uuid4(), 'patent_not_granted': 0, 'patent_granted': 0}]  # noqa: E501 # fmt: skip


def guidance_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT(*) as count_nature
        FROM guidance g
        WHERE g.year >= %(year)s
        GROUP BY nature, g.status, g.researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    guidance = pd.DataFrame(result)

    guidance = guidance.pivot_table(
        index=['researcher_id'],
        columns='nature',
        values='count_nature',
        aggfunc='size',
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

    guidance.rename(columns=rename_dict, inplace=True)

    columns = list(rename_dict.values()) + ['researcher_id']
    guidance = guidance.reindex(columns, axis='columns', fill_value=0)
    return guidance.to_dict(orient='records')


def academic_degree_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, MIN(education_end) AS first_doc
        FROM education
        WHERE degree = 'DOCTORATE'
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL)

    academic_degree = pd.DataFrame(result)

    return academic_degree.to_dict(orient='records')


def book_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS book
        FROM bibliographic_production
        WHERE type = 'BOOK'
            AND year_ >= %(year)s
        GROUP BY researcher_id
        """

    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def book_chapter_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS book_chapter
        FROM bibliographic_production
        WHERE type = 'BOOK_CHAPTER'
            AND year_ >= %(year)s
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def software_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) software
        FROM public.software s
        WHERE s.year >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        """
    result = conn.select(SCRIPT_SQL)
    return result


def researcher_classification(researcher: pd.DataFrame) -> str:  # noqa: PLR0911, PLR0912, PLR0915
    YEAR_DOC = datetime.datetime.now().year - researcher['first_doc']
    QUALIS_A = researcher['A1'] + researcher['A2']
    QUALIS_A += researcher['A3'] + researcher['A4']

    QUALIS_B = researcher['B1'] + researcher['B2']
    QUALIS_B += researcher['B3'] + researcher['B4']

    QUALIS_NA = researcher['C'] + researcher['SQ']

    ARTICLES = QUALIS_A + QUALIS_B + QUALIS_NA

    ACADEMIC_PRODUCTION = ARTICLES
    ACADEMIC_PRODUCTION += researcher['book'] + researcher['book_chapter']

    DOC_GUIDANCE = researcher['d_completed'] + researcher['d_in_progress']
    MASTERS_GUIDANCE = researcher['m_completed'] + researcher['m_in_progress']

    PATENT = researcher['patent_granted'] + researcher['patent_not_granted']
    SOFTWARE = researcher['software']

    if YEAR_DOC >= 10:
        PROD_RULE = ACADEMIC_PRODUCTION >= 5 and researcher['A1'] >= 2
        GUIDANCE_RULE = DOC_GUIDANCE >= 4
        COMBINED_RULE = researcher['A1'] >= 1 and PATENT >= 1
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'A+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 5 and researcher['A1'] >= 1
        GUIDANCE_RULE = DOC_GUIDANCE >= 2
        COMBINED_RULE = PATENT >= 1
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'A'

    if YEAR_DOC >= 8:
        PROD_RULE = ACADEMIC_PRODUCTION >= 4 and QUALIS_A >= 2
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 2 or DOC_GUIDANCE >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'B+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 4 and QUALIS_A >= 1
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 2 or DOC_GUIDANCE >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'B'

    if YEAR_DOC >= 6:
        PROD_RULE = ACADEMIC_PRODUCTION >= 3 and QUALIS_A >= 2
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 1 or DOC_GUIDANCE >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'C+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 3 and QUALIS_A >= 1
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 1 or DOC_GUIDANCE >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'C'

    if YEAR_DOC >= 3:
        PROD_RULE = ACADEMIC_PRODUCTION >= 2 and QUALIS_A >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if PROD_RULE or COMBINED_RULE:
            return 'D+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 2 and ARTICLES >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if PROD_RULE or COMBINED_RULE:
            return 'D'
    if YEAR_DOC > 0:
        PROD_RULE = ACADEMIC_PRODUCTION >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if PROD_RULE or COMBINED_RULE:
            return 'E+'
    return 'E'


if __name__ == '__main__':
    YEAR_FILTER = 2019

    dataframe = pd.DataFrame(list_researchers())

    articles = pd.DataFrame(article_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(articles, how='left', on=['researcher_id'])

    patents = pd.DataFrame(patent_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(patents, how='left', on=['researcher_id'])

    guidances = pd.DataFrame(guidance_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(guidances, how='left', on=['researcher_id'])

    softwares = pd.DataFrame(software_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(softwares, how='left', on=['researcher_id'])

    book = pd.DataFrame(book_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(book, how='left', on=['researcher_id'])

    book_chapter = pd.DataFrame(book_chapter_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(book_chapter, how='left', on=['researcher_id'])

    degree = pd.DataFrame(academic_degree_metrics())
    dataframe = dataframe.merge(degree, how='left', on=['researcher_id'])

    dataframe['class'] = dataframe.fillna(0).apply(
        researcher_classification, axis=1
    )
