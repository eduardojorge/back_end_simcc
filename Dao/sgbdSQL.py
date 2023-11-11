#import requests
import json
import pandas as pd
import psycopg2
import nltk
from nltk.tokenize import RegexpTokenizer
import unidecode
import sys

import project as project_






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
  #database_ ="cimatec_v7"
  #database_ ="simcc_profnit_v1"
  password_='root'
 
     
  if (project_.getProject())=="1":
     database_=  "cimatec_v7" 
     host_="172.25.0.84" 
     password_='wn6H4!16NBcb}4%hy6"h'

  if (project_.getProject())=="2":
     database_=  "simcc_profnit_v1" 
     host_="127.0.0.1" 
  if (project_.getProject())=="3":
     database_=  "simcc_ifba" 
     host_="127.0.0.1" 
  if (project_.getProject())=="4":
     database_=  "simcc_v4" 
     host_="127.0.0.1"    
  if (project_.getProject())=="5":
     database_=  "proforte" 
     host_="127.0.0.1"     
  if (project_.getProject())=="6":
     #tupi
     database_=  "simcc_" 
     host_="127.0.0.1"     
     
    

   


      
  #database_=sys.argv[1]
  #host_='172.25.0.84'
  #host_='127.0.0.1'
  #host_=sys.argv[2]
  #database_ ="simcc_profnit"
  #print(database_)
  
  con = psycopg2.connect(host=host_, 
                         #database='simcc_v3',
                         database=database_,
                         user='postgres', 
                         password=password_)
  return con


