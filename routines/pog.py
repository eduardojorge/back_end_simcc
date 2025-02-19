import logging
import os

from routines.logger import logger_routine
from simcc.repositories import conn

LOG_PATH = 'logs'

if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'pog.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article ba
        SET qualis = 'A4'
        WHERE ba.issn = '17412242';
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Qualis updated for ISSN 17412242')

    SCRIPT_SQL = """
        UPDATE researcher
        SET docente = true
        WHERE id IN (SELECT researcher_id FROM graduate_program_researcher);
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Docentes updated for graduate programs')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article p
        SET jcr=(subquery.jif2019), jcr_link=url_revista
        FROM (SELECT jif2019, eissn, url_revista FROM JCR) AS subquery
        WHERE translate(subquery.eissn,'-','') = p.issn
        """

    conn.exec(SCRIPT_SQL)
    logger.info('JCR updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article p
        SET jcr = (subquery.jif2019), jcr_link=url_revista
        FROM (SELECT jif2019, issn, url_revista FROM JCR) AS subquery
        WHERE translate(subquery.issn,'-','') = p.issn;
        """

    conn.exec(SCRIPT_SQL)
    logger.info('JCR updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET YEAR_ = YEAR::INTEGER
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Year updated for publications')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article
        SET qualis='B2'
        WHERE issn='26748568' OR issn='2764622'
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Qualis updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET title = translate(title, '''', ' ')
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Title updated for publications')
    logger_routine('POG', False)
