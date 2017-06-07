# Filename:    chart_drawer.py
# Date:        2017/06/07
# Description: generate bar chart and flowchart

import matplotlib.pyplot as plt
import db_wrapper
import pandas as pd
from graphviz import Digraph

# External function
# draw top 10 bar chart from lowest to highest
# inputData: DataFrame
# threshold: int
# dataType: enum{'scanningRate'}
def generateTop10LowestBarChart(inputData, threshold, dataType):
    orderedInput = inputData.sort_values(by=dataType, ascending=True)
    print(orderedInput)
    if (len(orderedInput) > 10):
        orderedInput = orderedInput[0:10]
    print(orderedInput)
    plt.axis([-1, len(orderedInput), 0, 100])
    plt.bar(range(len(orderedInput)), orderedInput[
            dataType], tick_label=orderedInput['station'])
    add_xy_labels(dataType)
    plt.hlines(threshold, -1, 10, colors='r', linestyles='solid')
    plt.text(len(orderedInput) + 0.2, threshold -
             1, 'threshold: ' + str(threshold))
    plt.show()

# External function
# generate flow chart
# tableName: String, the table you want to fetch the data
# startDate: String, format yyyy-mm-dd
# endDate: String, format yyyy-mm-dd
# fileName: String, the file used to save the flow chart picture, should end with ".gv"
def generateFlowChart(tableName, startDate, endDate, FileName):
    result = get_flow_chart_data(tableName, startDate, endDate)
    mColor = {
        "pass": "#1db70d",
        "fail": "#dc143c",
        "rework": "#3366cc",
        "scrap": "#ff0080",
        "pack": "brown",
        "finish": "#1db70d",
        "frak": "#ba030f",
        "scan": "black",
        "print": "#0ba5dd",
        "unpack": "black",
        "store": "darkgrey",
    }
    dot = Digraph(comment='Logviz')
    for i,d in result.iterrows():
        A = d[0]
        B = d[2]
        if (A is not None):
            aLabel = d[0] + " " + "%s" % d[4]
            bLabel = d[2] + " " + "%s" % d[4]
            eLabel = d[1] + " " + "%s" % d[3] + " " + "{:.1%}".format(d[5])
            eColor = mColor[d[1]]        
            fSize="8"
            if d[6]==1:
                fSize="16"        
            if d[5]>0.05:
                dot.node(A, aLabel)
                dot.node(B, bLabel)
                dot.edge(A, B, label=eLabel, color=eColor, fontcolor=eColor, fontsize=fSize)
    dot.render('temp/'+FileName+'.gv', view=True)

# Internal function

# Set the x label and y label according to data type
def add_xy_labels(dataType):
    yLabels = {
        'scanningRate': 'Scanning Rate',
    }
    plt.ylabel(yLabels.get(dataType, dataType))
    plt.xlabel('Station Name')

# get the statistical table used by flow chart. The table structure is below:
#   from_process from_event   to_process  qty  total      rate  row_number
# 0          None       None     2d-bc-le  898   1000  0.898000           1
# 1        s-extr       pass     2d-bc-le  102   1000  0.102000           2
def get_flow_chart_data(tableName, startDate, endDate):
    sql = """
    WITH args AS(
    SELECT
        'RSH'::Text AS site,
        'J80'::Text AS project,
        'Top Case'::Text AS component,
        '""" + startDate + """'::TIMESTAMP AS start_time,
        '""" + endDate + """'::TIMESTAMP AS end_time
    ),

    t_clean AS(
    SELECT
        tl.part_id,
        tl.process AS to_process,
        LAG(process) OVER (PARTITION BY part_id ORDER BY created) AS from_process,
        LAG(event) OVER (PARTITION BY part_id ORDER BY created) AS from_event
    FROM """ + tableName + """ tl
    WHERE tl.created>=(SELECT start_time FROM args) AND tl.created<(SELECT end_time FROM args)
    ),

    t_result AS(
    SELECT
        from_process,
        from_event,
        to_process,
        COUNT(part_id) AS qty    
    FROM t_clean
    GROUP BY 1,2,3
    )

    SELECT 
    *,
    (SUM(qty) OVER(PARTITION BY to_process))::INT AS total,
    qty/(SUM(qty) OVER(PARTITION BY to_process)) AS rate,
    row_number() OVER (PARTITION BY to_process ORDER BY qty DESC)
    FROM t_result 
    ORDER BY 3, 4 DESC;
    """
    flowChartData = db_wrapper.exeDB(sql)
    print(flowChartData)
    return flowChartData