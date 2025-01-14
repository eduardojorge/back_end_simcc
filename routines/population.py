import logging
import os

from simcc.repositories import conn

LOG_PATH = 'logs'


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        WHERE update_status = 'HOP_COMPLETED';
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

    researchers = list_researchers()
