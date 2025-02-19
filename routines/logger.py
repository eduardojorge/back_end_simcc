from simcc.repositories import conn


def logger_researcher_routine(researcher_id, routine_type, error, detail=None):
    params = {
        'researcher_id': researcher_id,
        'routine_type': routine_type,
        'error': error,
        'detail': detail,
    }
    SCRIPT_SQL = """
        INSERT INTO logs.researcher_routine
        (researcher_id, routine_type, error, detail)
        VALUES (%(researcher_id)s, %(routine_type)s, %(error)s, %(detail)s);
        """
    conn.exec(SCRIPT_SQL, params)


def logger_routine(routine_type, error, detail=None):
    params = {
        'routine_type': routine_type,
        'error': error,
        'detail': detail,
    }
    SCRIPT_SQL = """
        INSERT INTO logs.routine
        (routine_type, error, detail)
        VALUES (%(routine_type)s, %(error)s, %(detail)s);
        """
    conn.exec(SCRIPT_SQL, params)
