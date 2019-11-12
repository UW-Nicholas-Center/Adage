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


reader = csv.reader(open("ProfitabilityAndGrowth_1.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

grossProfit = []
lastYCQGrossProfit = []
ltmGrossProfit = []
lastLTMGrossProfit = []
qoqGrossProfitGrowth = []
yoyGrossProfitGrowth = []
ltmYoYGrossProfitGrowth = []

grossProfitMargin = []
lastYCQGrossProfitMargin = []
ltmGrossProfitMargin = []
lastLTMGrossProfitMargin = []
qoqGrossProfitMarginGrowth = []
yoyGrossProfitMarginGrowth = []
ltmYoYGrossProfitMarginGrowth = []

sga = []
lastYCQSGA= []
ltmSGA = []
lastLTMSGA = []
qoqSGAGrowth = []
yoySGAGrowth = []
ltmYoYSGAGrowth = []

sgaMargin = []
lastYCQSGAMargin= []
ltmSGAMargin = []
lastLTMSGAMargin = []
qoqSGAMarginGrowth = []
yoySGAMarginGrowth = []
ltmYoYSGAMarginGrowth = []

rd = []
lastYCQRD= []
ltmRD = []
lastLTMRD = []
qoqRDGrowth = []
yoyRDGrowth = []
ltmYoYRDGrowth = []

rdMargin = []
lastYCQRDMargin= []
ltmRDMargin = []
lastLTMRDMargin = []
qoqRDMarginGrowth = []
yoyRDMarginGrowth = []
ltmYoYRDMarginGrowth = []

errorNum = 0

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


    if(data[i][13] == "" or data[i][12] == ""):
        grossProfit.append(1)
    else:
        grossProfit.append(float(data[i][13])-float(data[i][12]))
    
    if(data[i][13] == "" or data[i][12] == "" or float(data[i][13]) == 0):
        grossProfitMargin.append(1)
    else:
        grossProfitMargin.append((float(data[i][13])-float(data[i][12]))/float(data[i][13]))
    
    if(data[i][15] == ""):
        sga.append(1)
    else:
        sga.append(float(data[i][15]))

    if(data[i][13] == "" or float(data[i][13]) == 0 or data[i][15] == ""):
        sgaMargin.append(1)
    else:
        sgaMargin.append(float(data[i][15])/float(data[i][13]))

    if(data[i][14] == ""):
        rd.append(1)
    else:
        rd.append(float(data[i][14]))

    if(data[i][14] == "" or data[i][13] == "" or float(data[i][13]) == 0):
        rdMargin.append(1)
    else:
        rdMargin.append(float(data[i][14])/float(data[i][13]))


lastYCQGrossProfit = lastYCQ(grossProfit)
ltmGrossProfit = ltmSum(grossProfit)
lastLTMGrossProfit = lastLTMSum(grossProfit)
qoqGrossProfitGrowth = qoqGrowth(grossProfit)
yoyGrossProfitGrowth = yoyGrowth(grossProfit)
ltmYoYGrossProfitGrowth = ltmYoYGrowth(grossProfit)
lastYCQGrossProfitMargin = lastYCQ(grossProfitMargin)
ltmGrossProfitMargin = ltmSum(grossProfitMargin)
lastLTMGrossProfitMargin = lastLTMSum(grossProfitMargin)
qoqGrossProfitMarginGrowth = qoqGrowth(grossProfitMargin)
yoyGrossProfitMarginGrowth = yoyGrowth(grossProfitMargin)
ltmYoYGrossProfitMarginGrowth = ltmYoYGrowth(grossProfitMargin)
lastYCQSGA= lastYCQ(sga)
ltmSGA = ltmSum(sga)
lastLTMSGA = lastLTMSum(sga)
qoqSGAGrowth = qoqGrowth(sga)
yoySGAGrowth = yoyGrowth(sga)
ltmYoYSGAGrowth = ltmYoYGrowth(sga)
lastYCQSGAMargin= lastYCQ(sgaMargin)
ltmSGAMargin = ltmSum(sgaMargin)
lastLTMSGAMargin = lastLTMSum(sgaMargin)
qoqSGAMarginGrowth = qoqGrowth(sgaMargin)
yoySGAMarginGrowth = yoyGrowth(sgaMargin)
ltmYoYSGAMarginGrowth = ltmYoYGrowth(sgaMargin)
lastYCQRD= lastYCQ(rd)
ltmRD = ltmSum(rd)
lastLTMRD = lastLTMSum(rd)
qoqRDGrowth = qoqGrowth(rd)
yoyRDGrowth = yoyGrowth(rd)
ltmYoYRDGrowth = ltmYoYGrowth(rd)
lastYCQRDMargin= lastYCQ(rdMargin)
ltmRDMargin = ltmSum(rdMargin)
lastLTMRDMargin = lastLTMSum(rdMargin)
qoqRDMarginGrowth = qoqGrowth(rdMargin)
yoyRDMarginGrowth = yoyGrowth(rdMargin)
ltmYoYRDMarginGrowth = ltmYoYGrowth(rdMargin)


finished = [tickers,dates,grossProfit,lastYCQGrossProfit,ltmGrossProfit,lastLTMGrossProfit,qoqGrossProfitGrowth,yoyGrossProfitGrowth,ltmYoYGrossProfitGrowth,grossProfitMargin,lastYCQGrossProfitMargin,ltmGrossProfitMargin,lastLTMGrossProfitMargin,qoqGrossProfitMarginGrowth,yoyGrossProfitMarginGrowth,ltmYoYGrossProfitMarginGrowth,sga,lastYCQSGA,ltmSGA,lastLTMSGA,qoqSGAGrowth,yoySGAGrowth,ltmYoYSGAGrowth,sgaMargin,lastYCQSGAMargin,ltmSGAMargin,lastLTMSGAMargin,qoqSGAMarginGrowth,yoySGAMarginGrowth,ltmYoYSGAMarginGrowth,rd,lastYCQRD,ltmRD,lastLTMRD,qoqRDGrowth,yoyRDGrowth,ltmYoYRDGrowth,rdMargin,lastYCQRDMargin,ltmRDMargin,lastLTMRDMargin,qoqRDMarginGrowth,yoyRDMarginGrowth,ltmYoYRDMarginGrowth]
finished = np.transpose(finished)

print(len(finished))
print(len(finished[0]))
import sqlite3

connection = sqlite3.connect("profitability.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists pg1 (
    ticker varchar,
    date varchar,
    grossProfit number,
    lastYCQGrossProfit number,
    ltmGrossProfit number,
    lastLTMGrossProfit number,
    qoqGrossProfitGrowth number,
    yoyGrossProfitGrowth number,
    ltmYoYGrossProfitGrowth number,
    grossProfitMargin number,
    lastYCQGrossProfitMargin number,
    ltmGrossProfitMargin number,
    lastLTMGrossProfitMargin number,
    qoqGrossProfitMarginGrowth number,
    yoyGrossProfitMarginGrowth number,
    ltmYoYGrossProfitMarginGrowth number,
    sga number,
    lastYCQSGA number,
    ltmSGA number,
    lastLTMSGA number,
    qoqSGAGrowth number,
    yoySGAGrowth number,
    ltmYoYSGAGrowth number,
    sgaMargin number,
    lastYCQSGAMargin number,
    ltmSGAMargin number,
    lastLTMSGAMargin number,
    qoqSGAMarginGrowth number,
    yoySGAMarginGrowth number,
    ltmYoYSGAMarginGrowth number,
    rd number,
    lastYCQRD number,
    ltmRD number,
    lastLTMRD number,
    qoqRDGrowth number,
    yoyRDGrowth number,
    ltmYoYRDGrowth number,
    rdMargin number,
    lastYCQRDMargin number,
    ltmRDMargin number,
    lastLTMRDMargin number,
    qoqRDMarginGrowth number,
    yoyRDMarginGrowth number,
    ltmYoYRDMarginGrowth number
); """

clear_dict = """DROP TABLE pg1;"""
insertList = '''INSERT INTO pg1(ticker, date,grossProfit,lastYCQGrossProfit,ltmGrossProfit,lastLTMGrossProfit,qoqGrossProfitGrowth,yoyGrossProfitGrowth,ltmYoYGrossProfitGrowth,grossProfitMargin,lastYCQGrossProfitMargin,ltmGrossProfitMargin,lastLTMGrossProfitMargin,qoqGrossProfitMarginGrowth,yoyGrossProfitMarginGrowth,ltmYoYGrossProfitMarginGrowth,sga,lastYCQSGA,ltmSGA,lastLTMSGA,qoqSGAGrowth,yoySGAGrowth,ltmYoYSGAGrowth,sgaMargin,lastYCQSGAMargin,ltmSGAMargin,lastLTMSGAMargin,qoqSGAMarginGrowth,yoySGAMarginGrowth,ltmYoYSGAMarginGrowth,rd,lastYCQRD,ltmRD,lastLTMRD,qoqRDGrowth,yoyRDGrowth,ltmYoYRDGrowth,rdMargin,lastYCQRDMargin,ltmRDMargin,lastLTMRDMargin,qoqRDMarginGrowth,yoyRDMarginGrowth,ltmYoYRDMarginGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from pg1''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()