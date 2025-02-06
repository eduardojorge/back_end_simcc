from simcc.repositories import conn
from simcc.schemas import YearBarema


def lattes_list(names: list = None, lattes: list = None) -> dict:
    one = False
    params = {}

    filter_lattes = str()
    if lattes:
        params['lattes'] = lattes
        filter_lattes = 'AND lattes_id = ANY(%(lattes)s)'

    filter_names = str()
    if names:
        params['names'] = names
        filter_names = 'AND name = ANY(%(names)s)'

    SCRIPT_SQL = f"""
        SELECT id AS researcher_id, lattes_id, name AS researcher, lattes_10_id,
            graduation
        FROM researcher
        WHERE 1 = 1
            {filter_names}
            {filter_lattes}
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def production(lattes_list: list, year: YearBarema): ...
