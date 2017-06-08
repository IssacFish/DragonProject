# Filename:    main.py
# Date:        2017/05/29
# Description: The program entrance

import os
import sys
import db_wrapper
import scan_rate
import chart_generator
import frak_rate
import matplotlib.pyplot as plt
import unnormal_scrap_rate

global database_type
# Option：mysql pgsql remote
database_type = 'pgsql'


def scan_rate_analysis():
    tableName = """process"""
    nextProcessName = """polish-qc"""
    curProcessName = """d-cnc2"""
    processFlow = ['2d-bc-le', 's-extr', '2d-bc-qc',
                   'd-cnc2', 'polish-qc', 'd-cnc7', 'cnc7-qc', 'cnc7-pkg', 'sb-qc', 'a-cbn', 'a-glo', 'ano-qc', 'ano-pkg', 'd-cnc9', 'a-thk', 'cnc10-qc', 'cnc10-pkg', 'printing-qc', 'a-flt', 'a-xy', 'a-ldg', 'aim', 'fqc', 'fatp-lbl', 'si', 'fg-pkg']
    date = '2016-10-26'
    endDate = '2016-10-30'

    plt.figure(1)
    # scanRate = scan_rate.calcProcessScanRate(tableName, processFlow, date)
    # scan_rate.generateBarChart(scanRate)
    # scan_rate.generateFlowChart(tableName, date, endDate)
    # plt.figure(2)
    #frakRate = frak_rate.calcProcessFrakRate(tableName, processFlow, date)
    # frak_rate.generateBarChart(frakRate)
    #frak_rate.generateFlowChart(frakRate, tableName, date, endDate)
    unnormalScrapRate = unnormal_scrap_rate.calcProcessUnnormalScrapRate(tableName, processFlow, date)
    #unnormal_scrap_rate.generateBarChart(unnormalScrapRate)
    unnormal_scrap_rate.generateFlowChart(unnormalScrapRate, tableName, date, endDate)
    plt.show()



def main():
    print("Data analysis process is running!")

    global database_type
    db_wrapper.connectDB(database_type)

    # Data Analysis
    scan_rate_analysis()
    #chart_generator.generateFlowChart('process', '2016-10-24', '2016-10-25', 'ScanningRate')

    # Close database
    db_wrapper.closeDB(database_type)


if __name__ == "__main__":
    main()
