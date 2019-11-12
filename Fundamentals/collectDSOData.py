import yfinance as yf
from yahoofinancials import YahooFinancials
import csv
import numpy as np
import pandas
import statistics
import os
import datetime
import random
import time
import pandas as pd
import matplotlib.pyplot as plt

import progressbar

import scipy.optimize
import random
from numpy import matrix, array, zeros, empty, sqrt, ones, dot, append, mean, cov, transpose, linspace
from numpy.linalg import inv, pinv

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile


def lastYCQ(variable):
    results = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i]):
            results.append(variable[i-4])
        else:
            results.append(1)
    return results

def qoqGrowth(variable):
    results = []
    for i in range(0, len(tickers)):
        if(i > 1 and tickers[i-1] == tickers[i] and variable[i] != 1 and variable[i-1] != 1 and variable[i-1] > 0):
            results.append(variable[i]/variable[i-1]-1)
        else:
            results.append(0)
    return results

def yoyGrowth(variable):
    results = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i] and variable[i-4] > 0 and variable[i] != 1 and variable[i-4] != 1):
            results.append(variable[i]/variable[i-4]-1)
        else:
            results.append(0)
    return results

def ltmSum(variable):
    results = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i]):
            value = 0
            for j in range(0,4):
                if(variable[i-j] != ""):
                    value += variable[i-j]
            results.append(value)
        else:
            results.append(1)
    return results

def lastLTMSum(variable):
    tempResults = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i]):
            value = 0
            for j in range(0,4):
                if(variable[i-j] != ""):
                    value += variable[i-j]
            tempResults.append(value)
        else:
            tempResults.append(1)
    results = []
    for i in range(0, len(tickers)):
        if(i > 1 and tickers[i-1] == tickers[i]):
            results.append(tempResults[i-1])
        else:
            results.append(1)
    return results

def ltmYoYGrowth(variable):
    tempResults = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i]):
            value = 0
            for j in range(0,4):
                if(variable[i-j] != ""):
                    value += variable[i-j]
            tempResults.append(value)
        else:
            tempResults.append(1)
    results = []
    for i in range(0, len(tickers)):
        if(i > 4 and tickers[i-4] == tickers[i] and tempResults[i] != 1 and tempResults[i-4] != 0 and tempResults[i-4] != 1):
            results.append(tempResults[i]/tempResults[i-4]-1)
        else:
            results.append(1)
    return results


reader = csv.reader(open("daysOutstandingAndFCF.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

DIO = []
lastYCQDIO = []
ltmDIO = []
lastLTMDIO = []
qoqDIOGrowth = []
yoyDIOGrowth = []
ltmYoYDIOGrowth = []

DSO = []
lastYCQDSO = []
ltmDSO = []
lastLTMDSO = []
qoqDSOGrowth = []
yoyDSOGrowth = []
ltmYoYDSOGrowth = []

DPO = []
lastYCQDPO= []
ltmDPO = []
lastLTMDPO = []
qoqDPOGrowth = []
yoyDPOGrowth = []
ltmYoYDPOGrowth = []

FCF = []
lastYCQFCF= []
ltmFCF = []
lastLTMFCF = []
qoqFCFGrowth = []
yoyFCFGrowth = []
ltmYoYFCFGrowth = []

FCFMargin = []
lastYCQFCFMargin= []
ltmFCFMargin = []
lastLTMFCFMargin = []
qoqFCFMarginGrowth = []
yoyFCFMarginGrowth = []
ltmYoYFCFMarginGrowth = []

errorNum = 0
missingNum = 1.23456
for i in range(0, len(data)):
    tickers.append(data[i][8])
    year = data[i][1][0:4]
    month = float(data[i][1][4:6])

    if(month < 4):
        dates.append("" + year + "0331")
    elif(month < 7):
        dates.append("" + year + "0630")
    elif(month < 10):
        dates.append("" + year + "0930")
    else:
        dates.append("" + year + "1231")


    if(data[i][15] == "" or data[i][13] == "" or float(data[i][13]) == 0):
        DIO.append(missingNum)
    else:
        DIO.append(365*float(data[i][15])/float(data[i][13]))
    
    if(data[i][17] == "" or data[i][20] == "" or data[i][19] == "" or float(data[i][19]) == 0):
        DSO.append(missingNum)
    else:
        DSO.append(365*float(data[i][17])/float(data[i][19]))

    if(data[i][12] == "" or data[i][13] == "" or float(data[i][13]) == 0):
        DPO.append(missingNum)
    else:
        DPO.append(365*float(data[i][12])/float(data[i][13]))

    if(data[i][22] == "" or data[i][16] == "" or data[i][15] == ""):
        FCF.append(missingNum)
    else:
        if(float(data[i][3]) == 1):
            capex = float(data[i][22])
        elif(float(data[i][3]) == 2):
            capex = float(data[i][22])/2
        elif(float(data[i][3]) == 3):
            capex = float(data[i][22])/3
        else:
            capex = float(data[i][22])/4
        FCF.append(float(data[i][16])+float(data[i][15])-capex)

    if(data[i][22] == "" or data[i][16] == "" or data[i][15] == "" or data[i][19] == "" or float(data[i][19]) == 0):
        FCFMargin.append(missingNum)
    else:
        FCFMargin.append((float(data[i][16])+float(data[i][15])-capex)/float(data[i][19]))


lastYCQDIO = lastYCQ(DIO)
ltmDIO = ltmSum(DIO)
lastLTMDIO = lastLTMSum(DIO)
qoqDIOGrowth = qoqGrowth(DIO)
yoyDIOGrowth = yoyGrowth(DIO)
ltmYoYDIOGrowth = ltmYoYGrowth(DIO)
lastYCQDSO = lastYCQ(DSO)
ltmDSO = ltmSum(DSO)
lastLTMDSO = lastLTMSum(DSO)
qoqDSOGrowth = qoqGrowth(DSO)
yoyDSOGrowth = yoyGrowth(DSO)
ltmYoYDSOGrowth = ltmYoYGrowth(DSO)
lastYCQDPO= lastYCQ(DPO)
ltmDPO = ltmSum(DPO)
lastLTMDPO = lastLTMSum(DPO)
qoqDPOGrowth = qoqGrowth(DPO)
yoyDPOGrowth = yoyGrowth(DPO)
ltmYoYDPOGrowth = ltmYoYGrowth(DPO)
lastYCQFCF= lastYCQ(FCF)
ltmFCF = ltmSum(FCF)
lastLTMFCF = lastLTMSum(FCF)
qoqFCFGrowth = qoqGrowth(FCF)
yoyFCFGrowth = yoyGrowth(FCF)
ltmYoYFCFGrowth = ltmYoYGrowth(FCF)
lastYCQFCFMargin = lastYCQ(FCFMargin)
ltmFCFMargin = ltmSum(FCFMargin)
lastLTMFCFMargin = lastLTMSum(FCFMargin)
qoqFCFMarginGrowth = qoqGrowth(FCFMargin)
yoyFCFMarginGrowth = yoyGrowth(FCFMargin)
ltmYoYFCFMarginGrowth = ltmYoYGrowth(FCFMargin)


fiDIOshed = [tickers,dates,DIO,lastYCQDIO,ltmDIO,lastLTMDIO,qoqDIOGrowth,yoyDIOGrowth,ltmYoYDIOGrowth,DSO,lastYCQDSO,ltmDSO,lastLTMDSO,qoqDSOGrowth,yoyDSOGrowth,ltmYoYDSOGrowth,DPO,lastYCQDPO,ltmDPO,lastLTMDPO,qoqDPOGrowth,yoyDPOGrowth,ltmYoYDPOGrowth,FCF,lastYCQFCF,ltmFCF,lastLTMFCF,qoqFCFGrowth,yoyFCFGrowth,ltmYoYFCFGrowth,FCFMargin,lastYCQFCFMargin,ltmFCFMargin,lastLTMFCFMargin,qoqFCFMarginGrowth,yoyFCFMarginGrowth,ltmYoYFCFMarginGrowth]



fiDIOshed = np.transpose(fiDIOshed)

print(len(fiDIOshed))
print(len(fiDIOshed[0]))
import sqlite3

connection = sqlite3.connect("daysOutstandingAndFCF.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists daysOutstandingAndFCF (
    ticker varchar,
    date varchar,
    DIO number,
    lastYCQDIO number,
    ltmDIO number,
    lastLTMDIO number,
    qoqDIOGrowth number,
    yoyDIOGrowth number,
    ltmYoYDIOGrowth number,
    DSO number,
    lastYCQDSO number,
    ltmDSO number,
    lastLTMDSO number,
    qoqDSOGrowth number,
    yoyDSOGrowth number,
    ltmYoYDSOGrowth number,
    DPO number,
    lastYCQDPO number,
    ltmDPO number,
    lastLTMDPO number,
    qoqDPOGrowth number,
    yoyDPOGrowth number,
    ltmYoYDPOGrowth number,
    FCF number,
    lastYCQFCF number,
    ltmFCF number,
    lastLTMFCF number,
    qoqFCFGrowth number,
    yoyFCFGrowth number,
    ltmYoYFCFGrowth number,
    FCFMargin number,
    lastYCQFCFMargin number,
    ltmFCFMargin number,
    lastLTMFCFMargin number,
    qoqFCFMarginGrowth number,
    yoyFCFMarginGrowth number,
    ltmYoYFCFMarginGrowth number
); """

clear_dict = """DROP TABLE daysOutstandingAndFCF;"""
insertList = '''INSERT INTO daysOutstandingAndFCF(ticker, date,DIO,lastYCQDIO,ltmDIO,lastLTMDIO,qoqDIOGrowth,yoyDIOGrowth,ltmYoYDIOGrowth,DSO,lastYCQDSO,ltmDSO,lastLTMDSO,qoqDSOGrowth,yoyDSOGrowth,ltmYoYDSOGrowth,DPO,lastYCQDPO,ltmDPO,lastLTMDPO,qoqDPOGrowth,yoyDPOGrowth,ltmYoYDPOGrowth,FCF,lastYCQFCF,ltmFCF,lastLTMFCF,qoqFCFGrowth,yoyFCFGrowth,ltmYoYFCFGrowth,FCFMargin,lastYCQFCFMargin,ltmFCFMargin,lastLTMFCFMargin,qoqFCFMarginGrowth,yoyFCFMarginGrowth,ltmYoYFCFMarginGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in fiDIOshed:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from daysOutstandingAndFCF''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()





