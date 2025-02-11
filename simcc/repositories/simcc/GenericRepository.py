from simcc.repositories import conn
from simcc.schemas import YearBarema


def lattes_list(names: list = None, lattes: list = None) -> dict:
    one = False
    params = {}

    filter_lattes = str()
    if lattes:
        params['lattes'] = lattes
        filter_lattes = 'AND r.lattes_id = ANY(%(lattes)s)'

    filter_names = str()
    if names:
        params['names'] = names
        filter_names = 'AND r.name = ANY(%(names)s)'

    SCRIPT_SQL = f"""
        SELECT r.id AS researcher_id, r.lattes_id, r.name AS researcher,
            r.lattes_10_id, r.graduation, rp.city, i.name as university,
            rp.great_area AS area
        FROM researcher r
        LEFT JOIN researcher_production rp
            ON r.id = rp.researcher_id
        LEFT JOIN institution i
            ON r.institution_id = i.id
        WHERE 1 = 1
            {filter_names}
            {filter_lattes}
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def production(lattes_list: list, year: YearBarema): ...
