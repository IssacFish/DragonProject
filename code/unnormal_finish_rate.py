# Filename:    unnormal_finish_rate.py
# Date:        2017/06/14
# Description: Analyze the parts that come back to the process after finish

import db_wrapper
import chart_generator
import config
import pandas as pd
import math

# process unnormal finish steps
# {stepName: DataFrame['part_id', 'process', 'event']}
global unnormalFinishMatrix
global totalFinishCount
global totalUnnormalFinishCount

# External function
def calcProcessUnnormalFinishRate(tableName, processFlow, date):
    global unnormalFinishMatrix
    global totalFinishCount
    global totalUnnormalFinishCount
    unnormalFinishMatrix = {}
    totalFinishCount = 0
    totalUnnormalFinishCount = 0
    sqlFinishRecords = """select * from """ + tableName + \
        """ where part_id in (select distinct part_id from """ + tableName + """ where event='finish' and date='""" + date + """')"""
    finishRecords = db_wrapper.exeDB(sqlFinishRecords)
    unnormalFinishRateDataFrame = pd.DataFrame(columns=['station', 'unnormalFinishRate'])
    unnormalFinishRateDataFrame['station'] = processFlow
    for index in range(len(processFlow)):
        curUnnormalFinishRate = calc_unnormal_finish_rate(tableName, finishRecords, processFlow[index], processFlow, date)
        unnormalFinishRateDataFrame.at[index, 'unnormalFinishRate'] = curUnnormalFinishRate * 100
        print('station:', processFlow[index], 'rate:%f'%curUnnormalFinishRate)
    print('The process unnormal finish rate are:')
    print(unnormalFinishRateDataFrame)
    print('The process unnormal finish records are:')
    print(unnormalFinishMatrix)
    return unnormalFinishRateDataFrame

# External function
def calcProcessAverageUnnormalFinishRate():
    global totalFinishCount
    global totalUnnormalFinishCount
    return (float)(totalUnnormalFinishCount) / totalFinishCount

# External function
# draw top 10 bar chart from highest to lowest
# inputData: DataFrame
def generateBarChart(inputData, subplot):
    print("Draw unnormal finish rate bar chart")
    orderedInputData = inputData.sort_values(by='unnormalFinishRate', ascending=False).reset_index()
    maxRange = orderedInputData.at[0, 'unnormalFinishRate']
    maxRange = math.ceil((float)(maxRange)/10)*10
    print(orderedInputData)
    chart_generator.generateBarChart(orderedInputData, config.unnormalFinishRateThreshold, 'unnormalFinishRate', subplot, maxRange)


def analyzeOutliers():
    global unnormalFinishMatrix
    for curStation in unnormalFinishMatrix:
        curOutliers = unnormalFinishMatrix[curStation]
        outlierAnalysis = curOutliers.groupby(['process', 'event']).count()
        print('The outlier analysis for station', curStation, 'is:')
        print(outlierAnalysis)

# External function
def generateFlowChart(inputData, startDate, endDate):
    result = inputData[inputData.unnormalFinishRate > 0]
    stationList = result['station'].tolist()
    for index in range(len(stationList)):
        curStation = stationList[index]
        chart_generator.generateFlowChart(curStation, startDate, endDate, 'ALL', 'ALL', curStation+'_finish')

def calc_unnormal_finish_rate(tableName, inputData, curProcessName, processFlow, date):
    global unnormalFinishMatrix
    global totalFinishCount
    global totalUnnormalFinishCount
    normalFinishEvent = ['pack', 'print', 'sorting', 'finish']
    unnormalFinishCount = 0
    finishRecord = inputData[(inputData.event == 'finish') & (inputData.process == curProcessName)]
    finishRecord = finishRecord.drop_duplicates(['part_id'])
    finishCount = len(finishRecord)
    totalFinishCount = totalFinishCount + finishCount
    if (finishCount==0):
        return 0
    unnormalFinishRecords = pd.DataFrame()
    unnormalFinishDataFrame = pd.DataFrame(columns=['part_id', 'process', 'event'])
    for partIndex,partRow in finishRecord.iterrows():
        part_id = partRow['part_id']
        curPartRecords = inputData[inputData.part_id == part_id].sort_values(by='created').reset_index()
        isFinish = False
        for index,row in curPartRecords.iterrows():
            if(curPartRecords.at[index, 'process']==curProcessName and curPartRecords.at[index, 'event']=='finish' ):
                startIndex = index
                isFinish = True
            else:
                curProcessIndex = processFlow.index(curProcessName)
                if (isFinish and curPartRecords.at[index, 'event']=='finish' and curPartRecords.at[index, 'process'] in processFlow[curProcessIndex+1:len(processFlow)] and not curPartRecords.at[index, 'process']==curProcessName):
                    break
                if (isFinish and not curPartRecords.at[index, 'event'] in normalFinishEvent):
                    unnormalFinishCount = unnormalFinishCount + 1
                    unnormalFinishRecords = unnormalFinishRecords.append(curPartRecords[startIndex:index+2], ignore_index=True)
                    unnormalFinishRecord = {'part_id':curPartRecords.at[index, 'part_id'], 'process':curPartRecords.at[index, 'process'], 'event':curPartRecords.at[index,'event']}
                    unnormalFinishDataFrame = unnormalFinishDataFrame.append(unnormalFinishRecord, ignore_index=True)
                    break
    if not unnormalFinishRecords.empty:
        #db_wrapper.createTable(unnormalFinishRecords, curProcessName)
        unnormalFinishMatrix[curProcessName] = unnormalFinishDataFrame
    totalUnnormalFinishCount = totalUnnormalFinishCount + unnormalFinishCount
    return (float)(unnormalFinishCount) / finishCount