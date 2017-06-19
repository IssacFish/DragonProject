#Filename:    db_wrapper.py
#Date:        2017/05/29
#Description: Database process

import pymysql
import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine

global conn
global engine

#External Function
def connectDB(type):
  global engine
  if(type == 'mysql'):
    connect_mysql()
  elif(type == 'pgsql'):
    connect_pgsql()
    engine = create_engine('postgresql://postgres:root000@localhost:5432/postgres')
  elif(type == 'remote'):
    connect_remote()
    engine = create_engine('postgresql://4c5ac9464f6743390f2de66e0b37e37c:9e02418f0f4d12eb30e7e10d5b310a71@clgodb.cloud.md.apple.com:3097/62e66db0c9fe88cb7c150abe4f250a5a')
  else:
    print("No matched database type!!!")
    return

def closeDB(type):
  if(type == 'mysql') or (type == 'pgsql') or (type == 'remote'):
    close_database()
  else:
    return

def exeDB(sql):
  #print(sql)
  global conn
  result=pd.read_sql(sql,conn)
  return result

def createTable(dataFrame, tableName):
  global engine
  dataFrame.to_sql(tableName, engine, if_exists='replace', index=False)


#Internal Function
def connect_mysql():
  print("Connecting local mysql database.")
  global conn
  conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='factory')

def connect_pgsql():
  print("Connecting local pgsql database.")
  global conn
  conn = pg.connect(database="postgres", user="postgres", password="root000", host="localhost", port="5432")

def connect_remote():
  print("Connecting remote database.")
  global conn
  conn = pg.connect(database="62e66db0c9fe88cb7c150abe4f250a5a", user="4c5ac9464f6743390f2de66e0b37e37c", password="9e02418f0f4d12eb30e7e10d5b310a71", host="clgodb.cloud.md.apple.com", port="3097")

def close_database():
  print("Closing database.")
  global conn
  conn.close()
