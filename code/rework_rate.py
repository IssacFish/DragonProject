# Filename:    rework_rate.py
# Date:        2017/06/14
# Description: Rework Analysis for the whole process, not for every step

import db_wrapper
import chart_generator
import config
import pandas as pd
import datetime as dt

# the unnormal step before rework
# DataFrame['part_id', 'process', 'to_process', 'event']
global unnormalReworkMatrix

# External function
# calculate the ratio that a part reworked today has more than 1 times of rework before
def calcReworkCount(tableName, date):
    # endDate = dt.datetime.strptime(date, '%Y-%m-%d')
    # startDate = endDate - dt.timedelta(days = 14)
    # startDateStr = startDate.strftime('%Y-%m-%d')
    sqlTotalReworkCount = """select count(*) from (select distinct part_id from """ + tableName + """ where event='rework' and date='""" + date + """') as table2"""
    totalReworkCount = db_wrapper.exeDB(sqlTotalReworkCount)
    # sqlRepeatReworkRecord = """select part_id from """ + tableName + """ where event='rework' and date>='""" + startDateStr + """' and part_id in (select distinct part_id from """ + tableName + """ where date='""" + date + """' and event='rework') group by part_id having count(part_id)>1;"""
    sqlRepeatReworkRecord = """select part_id from """ + tableName + """ where event='rework' and part_id in (select distinct part_id from """ + tableName + """ where date='""" + date + """' and event='rework') group by part_id having count(part_id)>1;"""
    repeatReworkRecord = db_wrapper.exeDB(sqlRepeatReworkRecord)
    if (totalReworkCount.iat[0,0]==0):
        return 0
    print("The parts that have more than 1 rework times are:")
    print(repeatReworkRecord)
    return len(repeatReworkRecord)/totalReworkCount.iat[0,0]

# External function
def calcUnnormalReworkRate(tableName, date):
    global unnormalReworkMatrix
    unnormalReworkMatrix = pd.DataFrame(columns=['part_id', 'process', 'to_process', 'event'])
    sqlReworkRecord = """select distinct part_id from """ + tableName + """ where event='rework' and date='""" + date + """'"""
    reworkRecord = db_wrapper.exeDB(sqlReworkRecord)
    reworkRecordCount = len(reworkRecord)
    if (reworkRecordCount == 0):
        return 0
    for partIndex in range(reworkRecordCount):
        part_id = reworkRecord.at[partIndex, 'part_id']
        sqlCurReversedPartRecords = """select part_id, event, process from """ + tableName + """ where part_id='""" + part_id + """' order by created desc"""
        curReversedPartRecords = db_wrapper.exeDB(sqlCurReversedPartRecords)
        isRework = False
        for index in range(len(curReversedPartRecords)):
            if (curReversedPartRecords.at[index,'event']=='rework'):
                isRework = True
            else:
                if (isRework):
                    if (not curReversedPartRecords.at[index, 'event']=='fail'):
                        unnormalReworkRecord = {'part_id':part_id, 'process':curReversedPartRecords.at[index, 'process'],'to_process':curReversedPartRecords.at[index-1, 'process'], 'event':curReversedPartRecords.at[index, 'event']}
                        unnormalReworkMatrix = unnormalReworkMatrix.append(unnormalReworkRecord, ignore_index=True)
                    isRework = False
    print("""The unnormal rework records are:""")
    print(unnormalReworkMatrix)
    return len(unnormalReworkMatrix)/reworkRecordCount

# External function
# generate the flow chart for every process that have unnormal rework event
def generateFlowChart(tableName, startDate, endDate):
    global unnormalReworkMatrix
    fromList = unnormalReworkMatrix['process'].tolist()
    fromList.extend(unnormalReworkMatrix['to_process'].tolist())
    fromList = list(set(fromList))
    chart_generator.generateFlowChart(tableName, startDate, endDate, fromList, 'ALL', 'unnormalRework')