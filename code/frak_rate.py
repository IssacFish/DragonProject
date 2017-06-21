# Filename:    frak_rate.py
# Date:        2017/06/08
# Description: frack rate analysis

import pandas as pd
import db_wrapper
import chart_generator
import config
import math

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
# Calculate the process average frak rate
def calcProcessAverageFrakRate(tableName, date):
    return calc_process_average_frak_rate(tableName, date)

# External function
# draw top 10 bar chart from highest to lowest
# inputData: DataFrame
def generateBarChart(inputData, ax):
    print("Draw frak rate bar chart")
    orderedInputData = inputData.sort_values(by='frakRate', ascending=False).reset_index()
    maxRange = orderedInputData.at[0, 'frakRate']
    maxRange = math.ceil((float)(maxRange)/10)*10
    print(orderedInputData)
    chart_generator.generateBarChart(orderedInputData, config.frakRateThreshold, 'frakRate', ax, maxRange)

# External function
# generate the flow chart for every process that have 'frak' vent
def generateFlowChart(inputData, tableName, startDate, endDate):
    result = inputData[inputData.frakRate > config.frakRateThreshold]
    chart_generator.generateFlowChart(tableName, startDate, endDate, result['station'].tolist(), 'ALL', 'frakRate')

# Internal function
# calculate the frak rate for a specific work station
def calc_frak_rate(tableName, curProcessName, date):
    sqlFrakCount = """select count(*) from (select part_id from """ + tableName + \
        """ where process='""" + curProcessName + """' and event='frak' and date='""" + date + """') as table2"""
    frackCount = db_wrapper.exeDB(sqlFrakCount)
    sqlTotalCount = """select count(*) from (select distinct part_id from """ + tableName + \
        """ where process='""" + curProcessName + """' and date='""" + date +"""') as table2"""
    totalCount = db_wrapper.exeDB(sqlTotalCount)
    if (totalCount.iat[0,0]==0):
        return 0
    frakRate = (float)(frackCount.iat[0, 0]) / totalCount.iat[0, 0]
    return frakRate

# Internal function
# calculate the process average frak rate
def calc_process_average_frak_rate(tableName, date):
    sqlFrakCount = """select count(*) from (select part_id from """ + tableName + \
        """ where event='frak' and date='""" + date + """') as table2"""
    frackCount = db_wrapper.exeDB(sqlFrakCount)
    sqlTotalCount = """select count(*) from (select distinct part_id from """ + tableName + \
        """ where date='""" + date +"""') as table2"""
    totalCount = db_wrapper.exeDB(sqlTotalCount)
    if (totalCount.iat[0,0]==0):
        return 0
    frakRate = (float)(frackCount.iat[0, 0]) / totalCount.iat[0, 0]
    return frakRate
