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


reader = csv.reader(open("ProfitabilityAndGrowth_3.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]

reader = csv.reader(open("dates.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
datesData = np.array(list(reader))


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

AssetImpair = []
lastYCQAssetImpair = []
ltmAssetImpair = []
lastLTMAssetImpair = []
qoqAssetImpairGrowth = []
yoyAssetImpairGrowth = []
ltmYoYAssetImpairGrowth = []

ShareBasedPay = []
lastYCQShareBasedPay= []
ltmShareBasedPay = []
lastLTMShareBasedPay = []
qoqShareBasedPayGrowth = []
yoyShareBasedPayGrowth = []
ltmYoYShareBasedPayGrowth = []

RestrucExp = []
lastYCQRestrucExp= []
ltmRestrucExp = []
lastLTMRestrucExp = []
qoqRestrucExpGrowth = []
yoyRestrucExpGrowth = []
ltmYoYRestrucExpGrowth = []


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


    if(data[i][12] == ""):
        AssetImpair.append(missingNum)
    else:
        AssetImpair.append(float(data[i][12]))
    
    if(data[i][16] == ""):
        ShareBasedPay.append(missingNum)
    else:
        ShareBasedPay.append(float(data[i][16]))

    if(data[i][14] == ""):
        RestrucExp.append(missingNum)
    else:
        RestrucExp.append(float(data[i][14]))



lastYCQAssetImpair = lastYCQ(AssetImpair)
ltmAssetImpair = ltmSum(AssetImpair)
lastLTMAssetImpair = lastLTMSum(AssetImpair)
qoqAssetImpairGrowth = qoqGrowth(AssetImpair)
yoyAssetImpairGrowth = yoyGrowth(AssetImpair)
ltmYoYAssetImpairGrowth = ltmYoYGrowth(AssetImpair)
lastYCQShareBasedPay= lastYCQ(ShareBasedPay)
ltmShareBasedPay = ltmSum(ShareBasedPay)
lastLTMShareBasedPay = lastLTMSum(ShareBasedPay)
qoqShareBasedPayGrowth = qoqGrowth(ShareBasedPay)
yoyShareBasedPayGrowth = yoyGrowth(ShareBasedPay)
ltmYoYShareBasedPayGrowth = ltmYoYGrowth(ShareBasedPay)
lastYCQRestrucExp= lastYCQ(RestrucExp)
ltmRestrucExp = ltmSum(RestrucExp)
lastLTMRestrucExp = lastLTMSum(RestrucExp)
qoqRestrucExpGrowth = qoqGrowth(RestrucExp)
yoyRestrucExpGrowth = yoyGrowth(RestrucExp)
ltmYoYRestrucExpGrowth = ltmYoYGrowth(RestrucExp)

finished = [tickers,dates,AssetImpair,lastYCQAssetImpair,ltmAssetImpair,lastLTMAssetImpair,qoqAssetImpairGrowth,yoyAssetImpairGrowth,ltmYoYAssetImpairGrowth,ShareBasedPay,lastYCQShareBasedPay,ltmShareBasedPay,lastLTMShareBasedPay,qoqShareBasedPayGrowth,yoyShareBasedPayGrowth,ltmYoYShareBasedPayGrowth,RestrucExp,lastYCQRestrucExp,ltmRestrucExp,lastLTMRestrucExp,qoqRestrucExpGrowth,yoyRestrucExpGrowth,ltmYoYRestrucExpGrowth]
finishedAvg = [tickers,dates]
for i in range(2, len(finished)):
    currVar = finished[i]
    avgCurrVar = list(np.zeros(len(currVar)))
    for j in range(0, len(datesData)):
        print(datesData[j][0])
        currSum = 0
        currCount = 1
        for k in range(0,len(finished[i])):
            if(finished[1][k] == datesData[j][0] and currVar[k] != missingNum):
                currSum += currVar[k]
                currCount += 1
        currAvg = currSum/currCount
        for k in range(0,len(finished[i])):
            if(finished[1][k] == datesData[j][0]):
                avgCurrVar[k] = currAvg
    print(avgCurrVar)
    finishedAvg.append(avgCurrVar)

finished = np.transpose(finished)
finishedAvg = np.transpose(finishedAvg)
import sqlite3

connection = sqlite3.connect("profitabilityAvg.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists pg3 (
    ticker varchar,
    date varchar,
    AssetImpair number,
    lastYCQAssetImpair number,
    ltmAssetImpair number,
    lastLTMAssetImpair number,
    qoqAssetImpairGrowth number,
    yoyAssetImpairGrowth number,
    ltmYoYAssetImpairGrowth number,
    ShareBasedPay number,
    lastYCQShareBasedPay number,
    ltmShareBasedPay number,
    lastLTMShareBasedPay number,
    qoqShareBasedPayGrowth number,
    yoyShareBasedPayGrowth number,
    ltmYoYShareBasedPayGrowth number,
    RestrucExp number,
    lastYCQRestrucExp number,
    ltmRestrucExp number,
    lastLTMRestrucExp number,
    qoqRestrucExpGrowth number,
    yoyRestrucExpGrowth number,
    ltmYoYRestrucExpGrowth number
); """

clear_dict = """DROP TABLE pg3;"""
insertList = '''INSERT INTO pg3(ticker,date,AssetImpair,lastYCQAssetImpair,ltmAssetImpair,lastLTMAssetImpair,qoqAssetImpairGrowth,yoyAssetImpairGrowth,ltmYoYAssetImpairGrowth,ShareBasedPay,lastYCQShareBasedPay,ltmShareBasedPay,lastLTMShareBasedPay,qoqShareBasedPayGrowth,yoyShareBasedPayGrowth,ltmYoYShareBasedPayGrowth,RestrucExp,lastYCQRestrucExp,ltmRestrucExp,lastLTMRestrucExp,qoqRestrucExpGrowth,yoyRestrucExpGrowth,ltmYoYRestrucExpGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finishedAvg:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from pg3''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()