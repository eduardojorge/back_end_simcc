import logging
import os

import nltk

from simcc.repositories import conn

LOG_PATH = 'logs'


def create_researcher_article_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'ARTICLE';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
                FROM bibliographic_production b
                WHERE type = 'ARTICLE'),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'ARTICLE'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_book_chapter_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'BOOK_CHAPTER';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
                FROM bibliographic_production b
                WHERE type = 'BOOK_CHAPTER'),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'BOOK_CHAPTER'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_patent_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'PATENT';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(p.title,'-\.:,;''', ' '), '\s+') AS word
                FROM patent p),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'PATENT'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_event_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'SPEAKER';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(p.title,'-\.:,;''', ' '), '\s+') AS word
                FROM participation_events p
                WHERE type_participation IN ('Apresentação Oral', 'Conferencista', 'Moderador', 'Simposista')),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'SPEAKER'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_abstract_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'ABSTRACT';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(r.abstract,'-\.:,;''', ' '), '\s+') AS word
                FROM researcher r),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'ABSTRACT'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_book_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'BOOK';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
                FROM bibliographic_production b
                WHERE type = 'BOOK'),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'BOOK'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        WHERE 'HOP-UPDATED' = ANY(routine_status);
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'population.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    create_researcher_article_dictionary()
    create_researcher_book_dictionary()
    create_researcher_book_chapter_dictionary()

    create_researcher_abstract_dictionary()
    create_researcher_patent_dictionary()
    create_researcher_event_dictionary()
