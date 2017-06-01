#Filename:    db_wrapper.py
#Date:        2017/05/29
#Description: Database process

import pymysql
import psycopg2 as pg
import pandas as pd


global conn

#External Function
def connectDB(type):
  if(type == 'mysql'):
    #Connect mysql local database
    db_host='localhost'
    db_port=3306
    db_user='root'
    db_passwd='root'
    db_name='factory'
    connect_mysql(db_host,db_port,db_user,db_passwd,db_name)
  elif(type == 'pgsql'):
    db_host="localhost"
    db_port="5432"
    db_user="postgres"
    db_passwd="root000"
    db_name="postgres"
    connect_pgsql(db_host,db_port,db_user,db_passwd,db_name)
  else:
    print("No matched database type!!!")
    return

def closeDB(type):
  if(type == 'mysql'):
    close_mysql()
  elif(type == 'pgsql'):
    close_pgsql()
  else:
    return

def exeDB(sql):
  global conn
  result=pd.read_sql(sql,conn)
  return result


#Internal Function
def connect_mysql(hostname,port,user,password,dbname):
  print("Connecting mysql database.")
  global conn
  conn = pymysql.connect(host=hostname, port=port, user=user, passwd=password, db=dbname)

def close_mysql():
  print("Closing mysql database.")
  global conn
  conn.close()

def connect_pgsql(hostname,port,user,password,dbname):
  print("Connecting pgsql database.")
  global conn
  conn = pg.connect(hostname,port,user,password,dbname)

def close_pgsql():
  print("Closing pgsql database.")
  global conn
  conn.close()



