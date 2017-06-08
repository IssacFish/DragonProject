#Filename:    process.py
#Date:        2017/06/07
#Description: Main Process Analysis

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy
import time
from sklearn.cluster import Birch
from sklearn.cluster import KMeans
import db_wrapper

global pid_frame
pid_frame = pd.DataFrame(columns=['pid', 'pcount','ptime'])

#External Function
def calcProcessStation(table_name):
    select_pid(table_name)
    full_cluster()
    scriped_data = data_pivot()
    main_process = gen_mainprocess(table_name,scriped_data)
    return

#Internal Function
def select_pid(table_name):
    global pid_frame
    global station_frame
    sql = """SELECT DISTINCT part_id FROM """ + table_name+";"""
    result = db_wrapper.exeDB(sql)

    pid_frame['pid'] = result
    pid_frame = pid_frame.drop(0,)

    pid_count = len(pid_frame)
    print(pid_count)

    for i in range(1,pid_count):
        sql = """SELECT * FROM """ + table_name+ """ WHERE part_id = """ + "'"+ pid_frame.loc[i,'pid'] +"'"+" ORDER BY created;"""
        result = db_wrapper.exeDB(sql)
        collect_pid(result,i)


def collect_pid(dataframe,pos):
    global pid_frame
    t_start =  dataframe.loc[0,'created']
    timeArray1 = time.strptime(str(t_start), "%Y-%m-%d %H:%M:%S")
    timeStart = int(time.mktime(timeArray1))

    index = len(dataframe)-1
    t_end =  dataframe.loc[index,'created']
    timeArray2 = time.strptime(str(t_end), "%Y-%m-%d %H:%M:%S")
    timeEnd = int(time.mktime(timeArray2))

#Process time unit:hours
    process_time = (timeEnd - timeStart)/3600

    pid_frame.loc[pos,'pcount'] = len(dataframe)
    pid_frame.loc[pos,'ptime'] = process_time

def data_pivot():
    global pid_frame
    pcount_t = pd.pivot_table(pid_frame,index=["pcount"],values=["pid"],aggfunc=[len])
    cluster_t = pd.pivot_table(pid_frame,index=["cluster"],values=["pid"],aggfunc=[len])

    pcount_t = pcount_t['len']
    pcount_t = pcount_t.sort_values(by='pid',ascending=False)
    print(pcount_t.head())

    max_count = pcount_t[pcount_t['pid'] == max(pcount_t['pid'])].index.values
    print(max_count)

    cluster_t = cluster_t['len']
    cluster_t = cluster_t.sort_values(by='pid',ascending=False)
    print(cluster_t.head())

    classid = cluster_t[cluster_t['pid'] == max(cluster_t['pid'])].index.values
    print(classid)

    strip_data = pid_frame[pid_frame['cluster']==int(classid)]
    strip_data = strip_data[strip_data['pcount']==int(max_count)]

    return strip_data

def gen_mainprocess(table_name,dataframe):
    station_frame = pd.DataFrame()
    pid_matrix = dataframe['pid'].values
    for i in range(0,len(dataframe)-1):
        sql = """SELECT * FROM """ + table_name+ """ WHERE part_id = """ + "'"+ pid_matrix[i] +"'"+" ORDER BY created;"""
        result = db_wrapper.exeDB(sql)
        id = str(pid_matrix[i])
        station_frame[id]=result['process']

    total_count = len(station_frame)
    #print(station_frame)
    T = station_frame.T

    main_process = pd.DataFrame(columns=['station'])

    for i in range(0,total_count):
        parse_t = T.icol(i).value_counts()
        print("Process parse:",i+1)
        print(parse_t)
        station = str(parse_t[parse_t==max(parse_t)].index.values)
        main_process.loc[i+1] = station

    print(main_process)
    return main_process

def full_cluster():
    global pid_frame
    count = len(pid_frame)
    print(count)
    x = [([0] * 2) for i in range(count)]

    for i in range(1,count):
        x[i-1][0] = pid_frame.loc[i,'ptime']
        x[i-1][1] = pid_frame.loc[i,'pcount']

    clf = KMeans(n_clusters=4)
    y_pred = clf.fit_predict(x)

    pid_frame['cluster'] = y_pred

    print(pid_frame.head())

#Draw plot
    plt.scatter(pid_frame['ptime'], pid_frame['pcount'], c=y_pred, marker='o')
    plt.title("Kmeans-Process Data")
    plt.xlabel("Cycle Time(hours)")
    plt.ylabel("Process Points")
    #plt.legend(["A","B"])

    plt.show()



