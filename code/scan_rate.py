# Filename:    scan_rate.py
# Date:        2017/06/01
# Description: Data Analysis

import pandas as pd
import db_wrapper
import chart_generator

# External Function


def calcProcessScanRate(tableName, processFlow, date):
    scanRateDataFrame = pd.DataFrame(
        columns=['station', 'scanningRate'])
    scanRateDataFrame['station'] = processFlow
    for index in range(len(processFlow) - 1):
        curScanRate = calc(tableName, processFlow[
            index + 1], processFlow[index], date)
        scanRateDataFrame.at[index,
                             'scanningRate'] = curScanRate * 100
        print(processFlow[index])
    scanRateDataFrame.at[len(processFlow) - 1, 'scanningRate'] = float(100)
    print(scanRateDataFrame)
    print("Draw scanning rate bar graph")
    chart_generator.generateTop10LowestBarChart(
        scanRateDataFrame, 99, 'scanningRate')


# Internal Function
def calc(tableName, nextProcessName, curProcessName, date):
    sqlTotalCount = """select count(*) from (select distinct part_id from """ + tableName + \
        """ where process='""" + nextProcessName + \
        """' and date='""" + date + """') as table2"""
    totalCount = db_wrapper.exeDB(sqlTotalCount)
    sqlScanCount = """select count(*) from (select distinct part_id from """ + tableName + """ where process='""" + curProcessName + \
        """' and part_id in (select part_id from """ + tableName + \
        """ where process='""" + nextProcessName + \
        """' and date='""" + date + """')) as table2;"""
    scanCount = db_wrapper.exeDB(sqlScanCount)
    if (totalCount.iat[0, 0] == 0):
        scanRate = 1
    else:
        scanRate = scanCount.iat[0, 0] / totalCount.iat[0, 0]
    return scanRate
