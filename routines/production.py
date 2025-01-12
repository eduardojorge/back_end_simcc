import logging
import os

import numpy as np
import pandas as pd

from simcc.repositories import conn

LOG_PATH = 'logs'


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def delete_researcher_production():
    SCRIPT_SQL = """
        DELETE FROM researcher_production
        WHERE researcher_id IN
        (SELECT id FROM researcher WHERE update_status = 'NOT_UPDATED');
        """
    conn.exec(SCRIPT_SQL)


def bibliographic_production_count():
    SCRIPT_SQL = """
        SELECT researcher_id, type, COUNT(*)
        FROM bibliographic_production
        GROUP BY researcher_id, type;
        """
    result = conn.select(SCRIPT_SQL)

    bibliographic_production = pd.DataFrame(result)
    bibliographic_production = bibliographic_production.pivot_table(
        index='researcher_id', columns='type', aggfunc='sum', fill_value=0
    )
    bibliographic_production.columns = (
        bibliographic_production.columns.get_level_values(1)
    )
    bibliographic_production = bibliographic_production.reset_index()

    columns = [
        'researcher_id',
        'BOOK',
        'WORK_IN_EVENT',
        'ARTICLE',
        'BOOK_CHAPTER',
    ]

    bibliographic_production = bibliographic_production.reindex(
        columns, axis='columns', fill_value=0
    )
    return bibliographic_production.to_dict(orient='records')


def list_great_area():
    SCRIPT_SQL = """
        SELECT researcher_id, STRING_AGG(DISTINCT gae.name, ';') as area
        FROM great_area_expertise gae
        LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_speciality():
    SCRIPT_SQL = """
        SELECT r.researcher_id,
            STRING_AGG(asp.name || ' | ' || ae.name, '; ') AS area_specialty
        FROM researcher_area_expertise r
        RIGHT JOIN area_specialty asp ON asp.id = r.area_specialty_id
        LEFT JOIN area_expertise ae ON r.area_expertise_id = ae.id
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_software():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS software
        FROM software
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_brand():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS brand
        FROM brand
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_patent():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS patent
        FROM patent
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'production.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    delete_researcher_production()

    researchers = list_researchers()
    researchers = pd.DataFrame(researchers)

    bibliographic_production = bibliographic_production_count()
    bibliographic_production = pd.DataFrame(bibliographic_production)

    area_speciality = list_speciality()
    area_speciality = pd.DataFrame(area_speciality)

    great_area = list_great_area()
    great_area = pd.DataFrame(great_area)

    software = list_software()
    software = pd.DataFrame(software)

    brand = list_brand()
    brand = pd.DataFrame(brand)

    patent = list_patent()
    patent = pd.DataFrame(patent)

    researchers = researchers.merge(
        bibliographic_production, how='left', on='researcher_id'
    )
    researchers = researchers.merge(
        area_speciality, how='left', on='researcher_id'
    )
    researchers = researchers.merge(great_area, how='left', on='researcher_id')
    researchers = researchers.merge(software, how='left', on='researcher_id')
    researchers = researchers.merge(brand, how='left', on='researcher_id')
    researchers = researchers.merge(patent, how='left', on='researcher_id')

    researchers = researchers.replace(np.nan, None)

    print(researchers[['researcher_id', 'area', 'area_specialty']])
