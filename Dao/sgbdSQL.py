import psycopg2
from config import settings


def conecta_db(
    host=None,
    database=None,
    user=None,
    password=None,
    port=None,
):
    host = host if host is not None else settings.DATABASE_HOST
    database = database if database is not None else settings.DATABASE_NAME
    user = user if user is not None else settings.DATABASE_USER
    password = password if password is not None else settings.DATABASE_PASSWORD
    port = port if port is not None else settings.PORT

    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )


def execScript_db(sql, params=None, database=None, host=None, password=None, port=None):
    try:
        with conecta_db(database=database, host=host, password=password, port=port) as con, con.cursor() as cur:  # fmt: skip
            cur.execute(sql, params)
            con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        con.rollback()
        return 1


def consultar_db(sql, params=None, database=None, host=None, password=None, port=None):
    print(sql)
    try:
        with conecta_db(database=database, host=host, password=password, port=port) as con, con.cursor() as cur:  # fmt: skip
            cur.execute(sql, params)
            registros = cur.fetchall()
            return registros
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return 1
