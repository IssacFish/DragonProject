#Filename:    main.py
#Date:        2017/05/29
#Description: The program entrance

import os
import sys
import db_wrapper
import scan_rate

global database_type
#Option：mysql pgsql
database_type = 'mysql'


def scan_rate_analysis():
  tableName = """process"""
  nextProcessName = """polish-qc"""
  curProcessName = """d-cnc2"""
  processFlow = ['2d-bc-le', 's-extr', '2d-bc-qc',
                'd-cnc2', 'polish-qc', 'd-cnc7', 'cnc7-qc', 'cnc7-pkg', 'sb-qc', 'a-cbn', 'a-glo', 'ano-qc', 'ano-pkg', 'd-cnc9', 'a-thk', 'cnc10-qc', 'cnc10-pkg', 'printing-qc', 'a-flt', 'a-xy', 'a-ldg', 'aim', 'fqc', 'fatp-lbl', 'si', 'fg-pkg']

  scan_rate.calcProcessScanRate(tableName, processFlow)



def main():
  print("Data analysis process is running!")

  global database_type
  db_wrapper.connectDB(database_type)

  #Data Analysis
  scan_rate_analysis();

  #Close database
  db_wrapper.closeDB(database_type)


if __name__=="__main__":
  main()