import psycopg2
import os


def execScript_db(sql):
    con = conecta_db()
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()


def consultar_db(sql, database: str = None):
    try:
        if database:
            con = conecta_db(database=database)
        else:
            con = conecta_db()
        cur = con.cursor()
        cur.execute(sql)
        registros = cur.fetchall()
        con.close()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    return registros


def conecta_db(
    password=os.getenv("DATABASE_PASSWORD"),
    host=os.getenv("DATABASE_HOST"),
    database=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
):

    con = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )
    return con
