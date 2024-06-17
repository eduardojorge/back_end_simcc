# import requests
import psycopg2
import project as project
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


def consultar_db(sql, database=None):
    try:
        con = conecta_db(database=database)
        cur = con.cursor()
        cur.execute(sql)
        recset = cur.fetchall()
        registros = []
        for rec in recset:
            registros.append(rec)
        con.close()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    return registros


def conecta_db(
    password=os.getenv("DATABASE_PASSWORD") or "root",
    host=os.getenv("DATABASE_HOST") or "localhost",
    database=os.getenv("DATABASE_NAME") or "simcc_",
    user=os.getenv("DATABASE_USER") or "postgres",
):
    print(
        password,
        host,
        database,
        user,
    )
    con = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )
    return con
