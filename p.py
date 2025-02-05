from simcc.repositories import conn, conn_admin

SCRIPT_SQL = """
    SELECT name, lattes_id, extra_field FROM researcher WHERE extra_field  IS NOT NULL;
    """
researchers = conn_admin(SCRIPT_SQL)

for researcher in researchers:
    SCRIPT_SQL = """
        UPDATE researcher SET extra_field = %(extra_field)s WHERE lattes_id = %(lattes_id)s
        """
    conn.exec(SCRIPT_SQL, researcher)
