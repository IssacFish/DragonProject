# Filename:    scan_rate.py
# Date:        2017/06/01
# Description: Data Analysis

import pandas as pd
import db_wrapper
import chart_generator
import config

# External Function
# tableName: String
# processFlow: ['...','...','...',...]
# date: String, yyyy-mm-dd
# output: table, bar chart and overview work flow
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
    return scanRateDataFrame

# External function
# draw top 10 bar chart from lowest to highest
# inputData: DataFrame
# dataType: enum{'scanningRate', 'frakRate'}
def generateBarChart(inputData):
    print("Draw scanning rate bar chart")
    orderedInputData = inputData.sort_values(by='scanningRate', ascending=True)
    print(orderedInputData)
    chart_generator.generateBarChart(orderedInputData, config.scanningRateThreshold, 'scanningRate')

# External function
# generate the overview flow chart
def generateFlowChart(tableName, startDate, endDate):
    chart_generator.generateFlowChart(tableName, startDate, endDate, 'ALL', 'ALL', 'overview')


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
    print(scanCount.iat[0,0])
    print(totalCount.iat[0,0])
    if (totalCount.iat[0, 0] == 0):
        scanRate = 1
    else:
        scanRate = (float)(scanCount.iat[0, 0]) / totalCount.iat[0, 0]
    return scanRate
