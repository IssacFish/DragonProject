# Filename:    unnormal_scrap_rate.py
# Date:        2017/06/08
# Description: unnormal scrap rate analysis, used to measure the scrap parts that come back to the normal work flow again

import db_wrapper
import chart_generator
import config
import pandas as pd

# process unnormal scrap steps
# {stepName: DataFrame['part_id', 'process', 'event']}
global unnormalScrapMatrix

#External function
def calcProcessUnnormalScrapRate(tableName, processFlow, date):
    global unnormalScrapMatrix
    unnormalScrapMatrix = {}
    sqlScrapRecords = """select * from """ + tableName + \
        """ where part_id in (select distinct part_id from """ + tableName + """ where event='scrap' and date='""" + date + """')"""
    scrapRecords = db_wrapper.exeDB(sqlScrapRecords)
    print(scrapRecords)
    unnormalScrapRateDataFrame = pd.DataFrame(columns=['station', 'unnormalScrapRate'])
    unnormalScrapRateDataFrame['station'] = processFlow
    for index in range(len(processFlow)):
        curUnnormalScrapRate = calc_unnormal_scrap_rate(scrapRecords, processFlow[index], date)
        unnormalScrapRateDataFrame.at[index, 'unnormalScrapRate'] = curUnnormalScrapRate * 100
    print(unnormalScrapRateDataFrame)
    return unnormalScrapRateDataFrame

# External function
# draw top 10 bar chart from highest to lowest
# inputData: DataFrame
def generateBarChart(inputData):
    print("Draw unnormal scrap rate bar chart")
    orderedInputData = inputData.sort_values(by='unnormalScrapRate', ascending=False)
    print(orderedInputData)
    chart_generator.generateBarChart(orderedInputData, config.unnormalScrapRateThreshold, 'unnormalScrapRate')

# External function
# generate the flow chart for every process that have 'frak' vent
def generateFlowChart(inputData, tableName, startDate, endDate):
    global unnormalScrapMatrix
    result = inputData[inputData.unnormalScrapRate > config.unnormalScrapRateThreshold]
    unnormalStation = result['station'].tolist()
    for index in range(len(unnormalStation)):
        curStation = unnormalStation[index]
        unnormalRecords = unnormalScrapMatrix[curStation]
        unnormalRecords.drop_duplicates(['from_process'])
        toList = unnormalRecords['from_process'].tolist()
        if (curStation not in toList):
            toList.append(curStation)
        print(toList)
        chart_generator.generateFlowChart(tableName, startDate, endDate, toList, 'ALL', curStation+'_scrap')

    #chart_generator.generateFlowChart(tableName, startDate, endDate, result['station'].tolist(), 'ALL', 'frakRate')

# Internal function
def calc_unnormal_scrap_rate(inputData, curProcessName, date):
    global unnormalScrapMatrix
    normalScrapEvent = ['pack', 'scrap']
    unnormalScrapCount = 0
    #sqlScrapRecord = """select distinct part_id from """ + tableName + \
    #    """ where process='""" + curProcessName + """' and event='scrap' and date='""" + date + """'"""
    #scrapRecord = db_wrapper.exeDB(sqlScrapRecord)
    scrapRecord = inputData[(inputData.event == 'scrap') & (inputData.process == curProcessName)].reset_index()
    scrapRecord = scrapRecord.drop_duplicates(['part_id'])
    scrapCount = len(scrapRecord)
    if(scrapCount==0):
        return 0
    unnormalScrapDataFrame = pd.DataFrame(columns=['part_id', 'from_process', 'process', 'event'])
    for partIndex in range(scrapCount):
        part_id = scrapRecord.at[partIndex,'part_id']
        #sqlCurPartRecords = """select part_id, event, process from """ + tableName + """ where part_id='""" + part_id + """' order by created"""
        #curPartRecords = db_wrapper.exeDB(sqlCurPartRecords)
        curPartRecords = inputData[inputData.part_id == part_id].sort_values(by='created').reset_index()
        isScrap = False
        for index in range(len(curPartRecords)):
            if(curPartRecords.at[index, 'process']==curProcessName and curPartRecords.at[index, 'event']=='scrap' ):
                isScrap = True
            if(isScrap and not curPartRecords.at[index, 'event'] in normalScrapEvent):
                unnormalScrapCount = unnormalScrapCount + 1
                unnormalScrapRecord = {'part_id':part_id, 'from_process':curPartRecords.at[index-1, 'process'],'process':curPartRecords.at[index, 'process'], 'event':curPartRecords.at[index, 'event']}
                unnormalScrapDataFrame = unnormalScrapDataFrame.append(unnormalScrapRecord, ignore_index=True)
                break
    unnormalScrapMatrix[curProcessName] = unnormalScrapDataFrame
    return (float)(unnormalScrapCount) / scrapCount