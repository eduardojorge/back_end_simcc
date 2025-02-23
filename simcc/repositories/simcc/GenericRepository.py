from uuid import UUID

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


def get_researcher_foment(institution_id: UUID):
    params = {}
    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    SCRIPT_SQL = f"""
        SELECT s.researcher_id, r.name, s.modality_code, s.modality_name,
            s.call_title, s.category_level_code, s.funding_program_name,
            s.institute_name, s.aid_quantity, s.scholarship_quantity
        FROM foment s
            LEFT JOIN researcher r
                ON s.researcher_id = r.id
        WHERE 1 = 1
            AND s.researcher_id IS NOT NULL
            AND researcher_id NOT IN
                (SELECT id FROM researcher WHERE docente = false)
            {filter_institution}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_logs():
    SCRIPT_SQL = """
        SELECT DISTINCT ON (routine_type) routine_type, error, detail,
            created_at
        FROM logs.routine
        ORDER BY routine_type, created_at DESC;
        """
    result = conn.select(SCRIPT_SQL)
    return result
