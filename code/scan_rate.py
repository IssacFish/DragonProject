#Filename:    scan_rate.py
#Date:        2017/06/01
#Description: Data Analysis

import matplotlib.pyplot as plt
import pandas as pd
import numpy
import scipy
import db_wrapper

#External Function
def calcProcessScanRate(tableName, processFlow):
    scanRateDataFrame = pd.DataFrame(
        columns=['station', 'scanningRate'])
    scanRateDataFrame['station'] = processFlow
    for index in range(len(processFlow) - 1):
        curScanRate = calc(tableName, processFlow[
                                   index + 1], processFlow[index])
        scanRateDataFrame.at[index,
                             'scanningRate'] = curScanRate * 100
        print(processFlow[index])
    scanRateDataFrame.at[len(processFlow) - 1, 'scanningRate'] = float(100)
    print(scanRateDataFrame)

    print("Draw scanning rate bar graph！")
    plt.barh(range(len(processFlow)), scanRateDataFrame.sort_index(ascending=False)['scanningRate'], tick_label=scanRateDataFrame.sort_index(ascending=False)['station'])
    plt.xlabel("Scanning Rate")
    plt.ylabel("Station Name")
    plt.show()


#Internal Function
def calc(tableName, nextProcessName, curProcessName):
    sqlTotalCount = """select count(*) from (select distinct part_id from """ + tableName + \
        """ where process='""" + nextProcessName + """') as polishTable"""
    totalCount = db_wrapper.exeDB(sqlTotalCount)
    # print(totalCount.head())
    sqlScanCount = """select count(*) from (select distinct part_id from """ + tableName + """ where process='""" + curProcessName + \
        """' and part_id in (select part_id from """ + tableName + \
        """ where process='""" + nextProcessName + """')) as table2;"""
    scanCount = db_wrapper.exeDB(sqlScanCount)
    # print(scanCount.iat[0,0])
    scanRate = scanCount.iat[0, 0] / totalCount.iat[0, 0]
    print(scanRate)
    return scanRate



