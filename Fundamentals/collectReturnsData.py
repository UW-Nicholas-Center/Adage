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


reader = csv.reader(open("returns.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

investedCap = []
lastYCQInvestedCap = []
qoqInvestedCapGrowth = []
yoyInvestedCapGrowth = []

roic = []
lastYCQROIC= []
ltmROIC = []
lastLTMROIC = []
qoqROICGrowth = []
yoyROICGrowth = []
ltmYOYROICGrowth = []

eva = []
lastYCQEVA = []
ltmEVA = []
lastLTMEVA = []
qoqEVAGrowth = []
yoyEVAGrowth = []
ltmYOYEVAGrowth = []

roe = []
lastYCQROE= []
ltmROE = []
lastLTMROE = []
qoqROEGrowth = []
yoyROEGrowth = []
ltmYOYROEGrowth = []


roa = []
lastYCQROA = []
ltmROA = []
lastLTMROA = []
qoqROAGrowth = []
yoyROAGrowth = []
ltmYOYROAGrowth = []

deprecFactor = []
lastYCQDeprecFactor = []
ltmDeprecFactor = []
lastLTMDeprecFactor = []
qoqDeprecFactorGrowth = []
yoyDeprecFactorGrowth = []
ltmYOYDeprecFactorGrowth = []

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


    if(data[i][13] == "" or data[i][14] == "" or data[i][15] == ""):
        investedCap.append(1)
    else:
        investedCap.append(float(data[i][13])+float(data[i][14])+float(data[i][15]))
    
    if(data[i][18] == "" or investedCap[i] == 0):
        roic.append(1)
    else:
        roic.append((float(data[i][18])/investedCap[i]))
    
    if(data[i][12] == "" or data[i][17] == ""):
        eva.append(1)
    else:
        eva.append(float(data[i][12])-float(data[i][17]))

    if(data[i][18] == "" or data[i][13] == "" or float(data[i][13]) == 0):
        roe.append(1)
    else:
        roe.append(float(data[i][18])/float(data[i][13]))

    if(data[i][18] == "" or data[i][12] == "" or float(data[i][12]) == 0):
        roa.append(1)
    else:
        roa.append(float(data[i][18])/float(data[i][12]))

    if(data[i][16] == "" or data[i][21] == "" or float(data[i][21]) == 0):
        deprecFactor.append(1)
    else:
        deprecFactor.append(float(data[i][16])/float(data[i][21]))



lastYCQInvestedCap = lastYCQ(investedCap)
qoqInvestedCapGrowth = qoqGrowth(investedCap)
yoyInvestedCapGrowth = yoyGrowth(investedCap)
lastYCQROIC= lastYCQ(roic)
ltmROIC = ltmSum(roic)
lastLTMROIC = lastLTMSum(roic)
qoqROICGrowth = qoqGrowth(roic)
yoyROICGrowth = yoyGrowth(roic)
ltmYOYROICGrowth = ltmYoYGrowth(roic)
lastYCQEVA = lastYCQ(eva)
ltmEVA = ltmSum(eva)
lastLTMEVA = lastLTMSum(eva)
qoqEVAGrowth = qoqGrowth(eva)
yoyEVAGrowth = yoyGrowth(eva)
ltmYOYEVAGrowth = ltmYoYGrowth(eva)
lastYCQROE= lastYCQ(roe)
ltmROE = ltmSum(roe)
lastLTMROE = lastLTMSum(roe)
qoqROEGrowth = qoqGrowth(roe)
yoyROEGrowth = yoyGrowth(roe)
ltmYOYROEGrowth = ltmYoYGrowth(roe)
lastYCQROA = lastYCQ(roa)
ltmROA = ltmSum(roa)
lastLTMROA = lastLTMSum(roa)
qoqROAGrowth = qoqGrowth(roa)
yoyROAGrowth = yoyGrowth(roa)
ltmYOYROAGrowth = ltmYoYGrowth(roa)
lastYCQDeprecFactor = lastYCQ(deprecFactor)
ltmDeprecFactor = ltmSum(deprecFactor)
lastLTMDeprecFactor = lastLTMSum(deprecFactor)
qoqDeprecFactorGrowth = qoqGrowth(deprecFactor)
yoyDeprecFactorGrowth = yoyGrowth(deprecFactor)
ltmYOYDeprecFactorGrowth = ltmYoYGrowth(deprecFactor)

finished = [tickers,dates,investedCap,lastYCQInvestedCap,yoyInvestedCapGrowth,roic,lastYCQROIC,ltmROIC,lastLTMROIC,qoqROICGrowth,yoyROICGrowth,ltmYOYROICGrowth,eva,lastYCQEVA,ltmEVA,lastLTMEVA,qoqEVAGrowth,yoyEVAGrowth,ltmYOYEVAGrowth,roe,lastYCQROE,ltmROE,lastLTMROE,qoqROEGrowth,yoyROEGrowth,ltmYOYROEGrowth,roa,lastYCQROA,ltmROA,lastLTMROA,qoqROAGrowth,yoyROAGrowth,ltmYOYROAGrowth,deprecFactor,lastYCQDeprecFactor,ltmDeprecFactor,lastLTMDeprecFactor,qoqDeprecFactorGrowth,yoyDeprecFactorGrowth,ltmYOYDeprecFactorGrowth]
finished = np.transpose(finished)


import sqlite3

connection = sqlite3.connect("returns.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists returns (
    ticker varchar,
    date varchar,
    investedCap number,
    lastYCQInvestedCap number,
    yoyInvestedCapGrowth number,
    roic number,
    lastYCQROIC number,
    ltmROIC number,
    lastLTMROIC number,
    qoqROICGrowth number,
    yoyROICGrowth number,
    ltmYOYROICGrowth number,
    eva number,
    lastYCQEVA number,
    ltmEVA number,
    lastLTMEVA number,
    qoqEVAGrowth number,
    yoyEVAGrowth number,
    ltmYOYEVAGrowth number,
    roe number,
    lastYCQROE number,
    ltmROE number,
    lastLTMROE number,
    qoqROEGrowth number,
    yoyROEGrowth number,
    ltmYOYROEGrowth number,
    roa number,
    lastYCQROA number,
    ltmROA number,
    lastLTMROA number,
    qoqROAGrowth number,
    yoyROAGrowth number,
    ltmYOYROAGrowth number,
    deprecFactor number,
    lastYCQDeprecFactor number,
    ltmDeprecFactor number,
    lastLTMDeprecFactor number,
    qoqDeprecFactorGrowth number,
    yoyDeprecFactorGrowth number,
    ltmYOYDeprecFactorGrowth number
); """

clear_dict = """DROP TABLE returns;"""
insertList = '''INSERT INTO returns(ticker, date,investedCap,lastYCQInvestedCap,yoyInvestedCapGrowth,roic,lastYCQROIC,ltmROIC,lastLTMROIC,qoqROICGrowth,yoyROICGrowth,ltmYOYROICGrowth,eva,lastYCQEVA,ltmEVA,lastLTMEVA,qoqEVAGrowth,yoyEVAGrowth,ltmYOYEVAGrowth,roe,lastYCQROE,ltmROE,lastLTMROE,qoqROEGrowth,yoyROEGrowth,ltmYOYROEGrowth,roa,lastYCQROA,ltmROA,lastLTMROA,qoqROAGrowth,yoyROAGrowth,ltmYOYROAGrowth,deprecFactor,lastYCQDeprecFactor,ltmDeprecFactor,lastLTMDeprecFactor,qoqDeprecFactorGrowth,yoyDeprecFactorGrowth,ltmYOYDeprecFactorGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from returns''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()