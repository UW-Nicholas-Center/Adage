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


reader = csv.reader(open("ProfitabilityAndGrowth_4.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

NI = []
lastYCQNI = []
ltmNI = []
lastLTMNI = []
qoqNIGrowth = []
yoyNIGrowth = []
ltmYoYNIGrowth = []

NIMargin = []
lastYCQNIMargin = []
ltmNIMargin = []
lastLTMNIMargin = []
qoqNIMarginGrowth = []
yoyNIMarginGrowth = []
ltmYoYNIMarginGrowth = []

DilutedEPS = []
lastYCQDilutedEPS= []
ltmDilutedEPS = []
lastLTMDilutedEPS = []
qoqDilutedEPSGrowth = []
yoyDilutedEPSGrowth = []
ltmYoYDilutedEPSGrowth = []

BasicEPS = []
lastYCQBasicEPS= []
ltmBasicEPS = []
lastLTMBasicEPS = []
qoqBasicEPSGrowth = []
yoyBasicEPSGrowth = []
ltmYoYBasicEPSGrowth = []

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


    if(data[i][20] == ""):
        NI.append(1)
    else:
        NI.append(float(data[i][20]))
    
    if(data[i][20] == "" or data[i][21] == "" or float(data[i][21]) == 0):
        NIMargin.append(1)
    else:
        NIMargin.append(float(data[i][20])/float(data[i][21]))

    if(data[i][14] == ""):
        DilutedEPS.append(1)
    else:
        DilutedEPS.append(float(data[i][14]))

    if(data[i][17] == ""):
        BasicEPS.append(1)
    else:
        BasicEPS.append(float(data[i][17]))


lastYCQNI = lastYCQ(NI)
ltmNI = ltmSum(NI)
lastLTMNI = lastLTMSum(NI)
qoqNIGrowth = qoqGrowth(NI)
yoyNIGrowth = yoyGrowth(NI)
ltmYoYNIGrowth = ltmYoYGrowth(NI)
lastYCQNIMargin = lastYCQ(NIMargin)
ltmNIMargin = ltmSum(NIMargin)
lastLTMNIMargin = lastLTMSum(NIMargin)
qoqNIMarginGrowth = qoqGrowth(NIMargin)
yoyNIMarginGrowth = yoyGrowth(NIMargin)
ltmYoYNIMarginGrowth = ltmYoYGrowth(NIMargin)
lastYCQDilutedEPS= lastYCQ(DilutedEPS)
ltmDilutedEPS = ltmSum(DilutedEPS)
lastLTMDilutedEPS = lastLTMSum(DilutedEPS)
qoqDilutedEPSGrowth = qoqGrowth(DilutedEPS)
yoyDilutedEPSGrowth = yoyGrowth(DilutedEPS)
ltmYoYDilutedEPSGrowth = ltmYoYGrowth(DilutedEPS)
lastYCQBasicEPS= lastYCQ(BasicEPS)
ltmBasicEPS = ltmSum(BasicEPS)
lastLTMBasicEPS = lastLTMSum(BasicEPS)
qoqBasicEPSGrowth = qoqGrowth(BasicEPS)
yoyBasicEPSGrowth = yoyGrowth(BasicEPS)
ltmYoYBasicEPSGrowth = ltmYoYGrowth(BasicEPS)


finished = [tickers,dates,NI,lastYCQNI,ltmNI,lastLTMNI,qoqNIGrowth,yoyNIGrowth,ltmYoYNIGrowth,NIMargin,lastYCQNIMargin,ltmNIMargin,lastLTMNIMargin,qoqNIMarginGrowth,yoyNIMarginGrowth,ltmYoYNIMarginGrowth,DilutedEPS,lastYCQDilutedEPS,ltmDilutedEPS,lastLTMDilutedEPS,qoqDilutedEPSGrowth,yoyDilutedEPSGrowth,ltmYoYDilutedEPSGrowth,BasicEPS,lastYCQBasicEPS,ltmBasicEPS,lastLTMBasicEPS,qoqBasicEPSGrowth,yoyBasicEPSGrowth,ltmYoYBasicEPSGrowth]
finished = np.transpose(finished)

print(len(finished))
print(len(finished[0]))
import sqlite3

connection = sqlite3.connect("profitability.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists pg4 (
    ticker varchar,
    date varchar,
    NI number,
    lastYCQNI number,
    ltmNI number,
    lastLTMNI number,
    qoqNIGrowth number,
    yoyNIGrowth number,
    ltmYoYNIGrowth number,
    NIMargin number,
    lastYCQNIMargin number,
    ltmNIMargin number,
    lastLTMNIMargin number,
    qoqNIMarginGrowth number,
    yoyNIMarginGrowth number,
    ltmYoYNIMarginGrowth number,
    DilutedEPS number,
    lastYCQDilutedEPS number,
    ltmDilutedEPS number,
    lastLTMDilutedEPS number,
    qoqDilutedEPSGrowth number,
    yoyDilutedEPSGrowth number,
    ltmYoYDilutedEPSGrowth number,
    BasicEPS number,
    lastYCQBasicEPS number,
    ltmBasicEPS number,
    lastLTMBasicEPS number,
    qoqBasicEPSGrowth number,
    yoyBasicEPSGrowth number,
    ltmYoYBasicEPSGrowth number
); """

clear_dict = """DROP TABLE pg4;"""
insertList = '''INSERT INTO pg4(ticker, date,NI,lastYCQNI,ltmNI,lastLTMNI,qoqNIGrowth,yoyNIGrowth,ltmYoYNIGrowth,NIMargin,lastYCQNIMargin,ltmNIMargin,lastLTMNIMargin,qoqNIMarginGrowth,yoyNIMarginGrowth,ltmYoYNIMarginGrowth,DilutedEPS,lastYCQDilutedEPS,ltmDilutedEPS,lastLTMDilutedEPS,qoqDilutedEPSGrowth,yoyDilutedEPSGrowth,ltmYoYDilutedEPSGrowth,BasicEPS,lastYCQBasicEPS,ltmBasicEPS,lastLTMBasicEPS,qoqBasicEPSGrowth,yoyBasicEPSGrowth,ltmYoYBasicEPSGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from pg4''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()