# import requests
import psycopg2
import project as project


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


def consultar_db(sql):
    try:
        con = conecta_db()
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


def conecta_db():
    password = "root"
    host = "localhost"

    if (project.getProject()) == "1":
        database = "cimatec_v7"
        host = "172.25.0.84"
        password = 'wn6H4!16NBcb}4%hy6"h'

    if (project.getProject()) == "2":
        database = "simcc_profnit_v1"

    if (project.getProject()) == "3":
        database = "simcc_ifba"

    if (project.getProject()) == "4":
        database = "simcc_"

    if (project.getProject()) == "5":
        database = "proforte"

    if (project.getProject()) == "6":
        database = "old_simcc_"

    if (project.getProject()) == "7":
        database = "inovacao"

    if (project.getProject()) == "8":
        database = "adm_simcc"

    con = psycopg2.connect(
        host=host,
        database=database,
        user="postgres",
        password=password,
    )
    return con
