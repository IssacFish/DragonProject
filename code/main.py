#Filename:    main.py
#Date:        2017/05/29
#Description: The program entrance

import os
import sys
import db_wrapper
import scan_rate
import chart_generator
import rework_rate

global database_type
#Option：mysql pgsql remote
database_type = 'remote'


def process_analysis():
  tableName="t_logs"
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
  date = '2016-12-08'
  endDate = '2016-12-09'

  #scan_rate.calcProcessScanRate(tableName, processFlow)
  #rework_rate.calcUnnormalReworkRate(tableName, date)
  #rework_rate.generateFlowChart(tableName, date, endDate)
  rework_rate.calcReworkCount(tableName, date)


def main():
  print("Data analysis process is running!")

  global database_type
  db_wrapper.connectDB(database_type)

  #Data Analysis
  #scan_rate_analysis()
  process_flow = process_analysis()


  #Close database
  db_wrapper.closeDB(database_type)


if __name__=="__main__":
  main()