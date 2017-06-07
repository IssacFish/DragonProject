# Filename:    frak_rate.py
# Date:        2017/06/08
# Description: frack rate analysis

import pandas as pd
import db_wrapper
import chart_generator
import config

# External function
def calcProcessFrakRate(tableName, processFlow, date):
    frakRateDataFrame = pd.DataFrame(columns=['station', 'frakRate'])
    frakRateDataFrame['station'] = processFlow
    for index in range(len(processFlow)):
        curFrakRate = calc_frak_rate(tableName, processFlow[index], date)
        frakRateDataFrame.at[index, 'frakRate'] = curFrakRate * 100
    print(frakRateDataFrame)
    return frakRateDataFrame

# External function
# draw top 10 bar chart from highest to lowest
# inputData: DataFrame
def generateBarChart(inputData):
    print("Draw frak rate bar chart")
    orderedInputData = inputData.sort_values(by='frakRate', ascending=False)
    print(orderedInputData)
    chart_generator.generateBarChart(orderedInputData, config.frakRateThreshold, 'frakRate')

# External function
# generate the overview flow chart
def generateFlowChart(inputData, tableName, startDate, endDate):
    result = inputData[inputData.frakRate > config.frakRateThreshold]
    chart_generator.generateFlowChart(tableName, startDate, endDate, result['station'].tolist(), 'ALL', 'frakRate')

# Internal function
def calc_frak_rate(tableName, curProcessName, date):
    sqlFrakCount = """select count(*) from (select part_id from """ + tableName + \
        """ where process='""" + curProcessName + """' and event='frak' and date='""" + date + """') as table2"""
    frackCount = db_wrapper.exeDB(sqlFrakCount)
    sqlTotalCount = """select count(*) from (select distinct part_id from """ + tableName + \
        """ where process='""" + curProcessName + """' and date='""" + date +"""') as table2"""
    totalCount = db_wrapper.exeDB(sqlTotalCount)
    if (totalCount.iat[0,0]==0):
        return 0
    frakRate = frackCount.iat[0, 0] / totalCount.iat[0, 0]
    return frakRate
