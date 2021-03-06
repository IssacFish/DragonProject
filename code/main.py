#Filename:    main.py
#Date:        2017/05/29
#Description: The program entrance

import os
import sys
import db_wrapper
import scan_rate
import chart_generator
import rework_rate
import matplotlib.pyplot as plt
import frak_rate
import unnormal_scrap_rate
import unnormal_finish_rate
import pandas as pd

global database_type
database_type = 'pgsql'


def process_analysis():
  tableName="process"
  startDate="2016-10-27"
  endDate="2016-10-28"
  data=chart_generator.get_flow_chart_data(tableName, startDate, endDate)
  return process.findMainProcess(data)


def scan_rate_analysis():
  #tableName = """t_logs"""
  tableName = """process"""
  nextProcessName = """polish-qc"""
  curProcessName = """d-cnc2"""
  processFlow = ['2d-bc-le', 's-extr', '2d-bc-qc',
                'd-cnc2', 'polish-qc', 'd-cnc7', 'cnc7-qc', 'cnc7-pkg', 'sb-qc', 'a-cbn', 'a-glo', 'ano-qc', 'ano-pkg', 'd-cnc9', 'a-thk', 'cnc10-qc', 'cnc10-pkg', 'printing-qc', 'a-flt', 'a-xy', 'a-ldg', 'aim', 'fqc', 'fatp-lbl', 'si', 'fg-pkg']
  date = '2016-10-26'
  endDate = '2016-11-16'

  fig = plt.figure(1)
  scanRateSubPlot = fig.add_subplot(2,2,1)
  frakRateSubPlot = fig.add_subplot(2,2,2)
  unnormalScrapRateSubPlot = fig.add_subplot(2,2,3)
  unnormalFinishRateSubPlot = fig.add_subplot(2,2,4)
  scanRate = scan_rate.calcProcessScanRate(tableName, processFlow, date)
  frakRate = frak_rate.calcProcessFrakRate(tableName, processFlow, date)
  unnormalScrapRate = unnormal_scrap_rate.calcProcessUnnormalScrapRate(tableName, processFlow, date)
  #print(unnormal_scrap_rate.calcProcessAverageUnnormalScrapRate())
  unnormalFinishRate = unnormal_finish_rate.calcProcessUnnormalFinishRate(tableName, processFlow, date)
  #print(unnormal_finish_rate.calcProcessAverageUnnormalFinishRate())
  scan_rate.generateBarChart(scanRate, scanRateSubPlot)
  frak_rate.generateBarChart(frakRate, frakRateSubPlot)
  unnormal_scrap_rate.generateBarChart(unnormalScrapRate, unnormalScrapRateSubPlot)
  unnormal_finish_rate.generateBarChart(unnormalFinishRate, unnormalFinishRateSubPlot)
  #result = pd.merge(unnormalScrapRate, frakRate).merge(scanRate).merge(unnormalFinishRate)
  #print(result)
  #unnormal_scrap_rate.calcProcessUnnormalScrapRate(tableName, processFlow, '2016-10-26')
  # unnormalReworkRate = rework_rate.calcUnnormalReworkRate(tableName, date)
  # print('The unnormal rework rate is: %f'%unnormalReworkRate)
  # #rework_rate.generateFlowChart(tableName, date, endDate)
  # reworkCount = rework_rate.calcReworkCount(tableName, date)
  # print('The rework count is: %f'%reworkCount)
  #print(frak_rate.calcProcessFrakRate(tableName, date))
  plt.show()


def main():
  print("Data analysis process is running!")

  global database_type
  db_wrapper.connectDB(database_type)

  #Data Analysis
  scan_rate_analysis()
  #process_flow = process_analysis()


  #Close database
  db_wrapper.closeDB(database_type)


if __name__=="__main__":
  main()