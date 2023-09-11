#import requests
import json
import pandas as pd
import psycopg2
import nltk
from nltk.tokenize import RegexpTokenizer
import unidecode

# Função para inserir dados no banco
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


# Função para consultas no banco
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
    

# Função para criar conexão no banco
def conecta_db():
  database_ ="cimatec_v7"
  #database_ ="simcc_profnit"
  #print(database_)
  con = psycopg2.connect(host='localhost', 
                         #database='simcc_v3',
                         database=database_,
                         user='postgres', 
                         password='root')
  return con