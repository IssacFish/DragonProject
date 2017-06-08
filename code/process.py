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
    cluster()

#Internal Function
def select_pid(table_name):
    global pid_frame
    sql = """SELECT DISTINCT part_id FROM """ + table_name+";"""
    result = db_wrapper.exeDB(sql)

    pid_frame['pid'] = result
    pid_frame = pid_frame.drop(0,)

    pid_count = len(pid_frame)
    print(pid_count)

    for i in range(1,pid_count):
        sql = """SELECT * FROM process WHERE part_id = """ + "'"+ pid_frame.loc[i,'pid'] +"'"+"ORDER BY created;"""
        result = db_wrapper.exeDB(sql)
        collect_pid(result,i)

    print(pid_frame)


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

def cluster():
    global pid_frame
    #pid_frame = pid_frame.loc[pid_frame.loc[:,"ptime"]<50,:]
    count = len(pid_frame)
    print(count)
    x = [([0] * 2) for i in range(count)]

    for i in range(1,count):
        x[i-1][0] = pid_frame.loc[i,'ptime']
        x[i-1][1] = pid_frame.loc[i,'pcount']

    clf = KMeans(n_clusters=2)
    y_pred = clf.fit_predict(x)
#Draw plot
    plt.scatter(pid_frame['ptime'], pid_frame['pcount'], c=y_pred, marker='o')
    plt.title("Kmeans-Process Data")
    plt.xlabel("Cycle Time(hours)")
    plt.ylabel("Process Points")
    #plt.legend(["A","B"])

    plt.show()



