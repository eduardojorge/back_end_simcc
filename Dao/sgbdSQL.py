import psycopg2
from config import settings


def conecta_db(database=None):
    return psycopg2.connect(
        host=settings.DATABASE_HOST,
        database=database if database else settings.DATABASE_NAME,
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        port=settings.PORT,
    )


def execScript_db(sql, params=None, database=None):
    con = conecta_db() if database is None else conecta_db(database=database)
    cur = con.cursor()
    try:
        cur.execute(sql, params)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        con.rollback()
        return 1
    finally:
        cur.close()
        con.close()


def consultar_db(sql, params=None, database=None):
    con = conecta_db() if database is None else conecta_db(database=database)
    cur = con.cursor()
    try:
        cur.execute(sql, params)
        registros = cur.fetchall()
        return registros
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return 1
    finally:
        cur.close()
        con.close()
