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


reader = csv.reader(open("capStructure.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]

quarterRevenue = []
LTMRevenue = []

tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

debt = []
lastYCQDebt = []
qoqDebtGrowth = []
yoyDebtGrowth = []
bookValue = []
lastYCQBookValue = []
qoqBookValueGrowth = []
yoyBookValueGrowth = []

debtOverCap = []
lastYCQDebtOverCap = []
qoqDebtOverCapGrowth = []
yoyDebtOverCapGrowth = []
debtOverEquity = []
lastYCQDebtOverEquity = []
qoqDebtOverEquityGrowth = []
yoyDebtOverEquityGrowth = []

assets = []
lastYCQAssets = []
qoqAssetsGrowth = []
yoyAssetsGrowth = []

interestCoverage = []
lastYCQInterestCoverage = []
ltmInterestCoverage = []
lastLTMInterestCoverage = []
qoqInterestCoverageGrowth = []
yoyInterestCoverageGrowth = []
ltmYOYInterestCoverageGrowth = []

quickRatio = []
lastYCQQuickRatio = []
ltmQuickRatio = []
lastLTMQuickRatio = []
qoqQuickRatioGrowth = []
yoyQuickRatioGrowth = []
ltmYOYQuickRatioGrowth = []

currentRatio = []
lastYCQCurrentRatio = []
ltmCurrentRatio = []
lastLTMCurrentRatio = []
qoqCurrentRatioGrowth = []
yoyCurrentRatioGrowth = []
ltmYOYCurrentRatioGrowth = []

assetTurnover = []
lastYCQAssetTurnover = []
ltmAssetTurnover = []
lastLTMAssetTurnover = []
qoqAssetTurnoverGrowth = []
yoyAssetTurnoverGrowth = []
ltmYOYAssetTurnoverGrowth = []

goodwillIntang = []
lastYCQGoodwillIntang = []
qoqGoodwillIntangGrowth = []
yoyGoodwillIntangGrowth = []
goodwillIntangOverAssets = []
lastYCQGoodwillIntangOverAssets = []
qoqGoodwillIntangOverAssetsGrowth = []
yoyGoodwillIntangOverAssetsGrowth = []


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


    if(data[i][24] == "" or float(data[i][24]) == 0):
        quarterRevenue.append(1)
    else:
        quarterRevenue.append(float(data[i][24]))
    
    if(data[i][15] == "" or data[i][16] == "" or (float(data[i][15]) == 0 and float(data[i][16] == 0))):
        debt.append(1)
    else:
        debt.append(float(data[i][15]) + float(data[i][16]))
    
    if(data[i][13] == "" or data[i][21] == ""):
        bookValue.append(1)
    else:
        bookValue.append(float(data[i][13])-float(data[i][21]))

    if(data[i][28] == "" or float(data[i][28]) == 0 or debt[i] == 1):
        debtOverCap.append(1)
    else:
        debtOverCap.append(debt[i]/float(data[i][28]))

    if(data[i][25] == "" or float(data[i][25]) == 0 or debt[i] == 1):
        debtOverEquity.append(1)
    else:
        debtOverEquity.append(debt[i]/float(data[i][25]))

    if(data[i][13] == "" or float(data[i][13]) == 0):
        assets.append(1)
    else:
        assets.append(float(data[i][13]))

    if(quarterRevenue[i] == 1 or data[i][26] == "" or float(data[i][26]) == 0):
        interestCoverage.append(1)
    else:
        interestCoverage.append(quarterRevenue[i]/float(data[i][26]))

    if(data[i][20] == "" or float(data[i][20]) == 0 or data[i][12] == "" or data[i][19] == ""):
        quickRatio.append(1)
    else:
        quickRatio.append((float(data[i][12])-float(data[i][19]))/float(data[i][20]))
    
    if(data[i][20] == "" or float(data[i][20]) == 0 or data[i][12] == ""):
        currentRatio.append(1)
    else:
        currentRatio.append(float(data[i][12])/float(data[i][20]))

    if(data[i][13] == "" or float(data[i][13]) == 0 or quarterRevenue[i] == 1):
        assetTurnover.append(1)
    else:
        assetTurnover.append(quarterRevenue[i]/float(data[i][13]))

    if(data[i][17] == "" and data[i][18] == ""):
        goodwillIntang.append(1)
    elif(data[i][17] == ""):
        goodwillIntang.append(float(data[i][18]))
    elif(data[i][18] == ""):
        goodwillIntang.append(float(data[i][17]))
    else:
        goodwillIntang.append(float(data[i][17])+float(data[i][18]))

    if(data[i][13] == "" or float(data[i][13]) == 0 or goodwillIntang[i] == 1):
        goodwillIntangOverAssets.append(1)
    else:
        goodwillIntangOverAssets.append(goodwillIntang[i]/float(data[i][13]))

    if(i > 4 and data[i][8] == data[i-4][8]):
        temp = 0
        for j in range(0,4):
            temp += quarterRevenue[i-j]
        LTMRevenue.append(temp)
    else:
        LTMRevenue.append(1)





lastYCQDebt = lastYCQ(debt)
qoqDebtGrowth = qoqGrowth(debt)
yoyDebtGrowth = yoyGrowth(debt)
lastYCQBookValue = lastYCQ(bookValue)
qoqBookValueGrowth = qoqGrowth(bookValue)
yoyBookValueGrowth = yoyGrowth(bookValue)
lastYCQDebtOverCap = lastYCQ(debtOverCap)
qoqDebtOverCapGrowth = qoqGrowth(debtOverCap)
yoyDebtOverCapGrowth = yoyGrowth(debtOverCap)
lastYCQDebtOverEquity = lastYCQ(debtOverEquity)
qoqDebtOverEquityGrowth = qoqGrowth(debtOverEquity)
yoyDebtOverEquityGrowth = yoyGrowth(debtOverEquity)
lastYCQAssets = lastYCQ(assets)
qoqAssetsGrowth = qoqGrowth(assets)
yoyAssetsGrowth = yoyGrowth(assets)
lastYCQInterestCoverage = lastYCQ(interestCoverage)
ltmInterestCoverage = ltmSum(interestCoverage)
lastLTMInterestCoverage = lastLTMSum(interestCoverage)
qoqInterestCoverageGrowth = qoqGrowth(interestCoverage)
yoyInterestCoverageGrowth = yoyGrowth(interestCoverage)
ltmYOYInterestCoverageGrowth = ltmYoYGrowth(interestCoverage)
lastYCQQuickRatio = lastYCQ(quickRatio)
ltmQuickRatio = ltmSum(quickRatio)
lastLTMQuickRatio = lastLTMSum(quickRatio)
qoqQuickRatioGrowth = qoqGrowth(quickRatio)
yoyQuickRatioGrowth = yoyGrowth(quickRatio)
ltmYOYQuickRatioGrowth = ltmYoYGrowth(quickRatio)
lastYCQCurrentRatio = lastYCQ(currentRatio)
ltmCurrentRatio = ltmSum(currentRatio)
lastLTMCurrentRatio = lastLTMSum(currentRatio)
qoqCurrentRatioGrowth = qoqGrowth(currentRatio)
yoyCurrentRatioGrowth = yoyGrowth(currentRatio)
ltmYOYCurrentRatioGrowth = ltmYoYGrowth(currentRatio)
lastYCQAssetTurnover = lastYCQ(assetTurnover)
ltmAssetTurnover = ltmSum(assetTurnover)
lastLTMAssetTurnover = lastLTMSum(assetTurnover)
qoqAssetTurnoverGrowth = qoqGrowth(assetTurnover)
yoyAssetTurnoverGrowth = yoyGrowth(assetTurnover)
ltmYOYAssetTurnoverGrowth = ltmYoYGrowth(assetTurnover)
lastYCQGoodwillIntang = lastYCQ(goodwillIntang)
qoqGoodwillIntangGrowth = qoqGrowth(goodwillIntang)
yoyGoodwillIntangGrowth = yoyGrowth(goodwillIntang)
lastYCQGoodwillIntangOverAssets = lastYCQ(goodwillIntangOverAssets)
qoqGoodwillIntangOverAssetsGrowth = qoqGrowth(goodwillIntangOverAssets)
yoyGoodwillIntangOverAssetsGrowth = yoyGrowth(goodwillIntangOverAssets)


finished = [tickers, dates,debt,bookValue,debtOverCap,debtOverEquity,assets,interestCoverage,quickRatio,currentRatio,assetTurnover,goodwillIntang,goodwillIntangOverAssets,lastYCQDebt,qoqDebtGrowth,yoyDebtGrowth,lastYCQBookValue,qoqBookValueGrowth,yoyBookValueGrowth,lastYCQDebtOverCap,qoqDebtOverCapGrowth,yoyDebtOverCapGrowth,lastYCQDebtOverEquity,qoqDebtOverEquityGrowth,yoyDebtOverEquityGrowth,lastYCQAssets,qoqAssetsGrowth,yoyAssetsGrowth,lastYCQInterestCoverage,ltmInterestCoverage,lastLTMInterestCoverage,qoqInterestCoverageGrowth,yoyInterestCoverageGrowth,ltmYOYInterestCoverageGrowth,lastYCQQuickRatio,ltmQuickRatio,lastLTMQuickRatio,qoqQuickRatioGrowth,yoyQuickRatioGrowth,ltmYOYQuickRatioGrowth,lastYCQCurrentRatio,ltmCurrentRatio,lastLTMCurrentRatio,qoqCurrentRatioGrowth,yoyCurrentRatioGrowth,ltmYOYCurrentRatioGrowth,lastYCQAssetTurnover,ltmAssetTurnover,lastLTMAssetTurnover,qoqAssetTurnoverGrowth,yoyAssetTurnoverGrowth,ltmYOYAssetTurnoverGrowth,lastYCQGoodwillIntang,qoqGoodwillIntangGrowth,yoyGoodwillIntangGrowth,lastYCQGoodwillIntangOverAssets,qoqGoodwillIntangOverAssetsGrowth,yoyGoodwillIntangOverAssetsGrowth]
finished = np.transpose(finished)


import sqlite3

connection = sqlite3.connect("capStructure.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists capStructure (
    ticker varchar,
    date varchar,
    debt number,
    bookValue number,
    debtOverCap number,
    debtOverEquity number,
    assets number,
    interestCoverage number,
    quickRatio number,
    currentRatio number,
    assetTurnover number,
    goodwillIntang number,
    goodwillIntangOverAssets number,
    lastYCQDebt number,
    qoqDebtGrowth number,
    yoyDebtGrowth number,
    lastYCQBookValue number,
    qoqBookValueGrowth number,
    yoyBookValueGrowth number,
    lastYCQDebtOverCap number,
    qoqDebtOverCapGrowth number,
    yoyDebtOverCapGrowth number,
    lastYCQDebtOverEquity number,
    qoqDebtOverEquityGrowth number,
    yoyDebtOverEquityGrowth number,
    lastYCQAssets number,
    qoqAssetsGrowth number,
    yoyAssetsGrowth number,
    lastYCQInterestCoverage number,
    ltmInterestCoverage number,
    lastLTMInterestCoverage number,
    qoqInterestCoverageGrowth number,
    yoyInterestCoverageGrowth number,
    ltmYOYInterestCoverageGrowth number,
    lastYCQQuickRatio number,
    ltmQuickRatio number,
    lastLTMQuickRatio number,
    qoqQuickRatioGrowth number,
    yoyQuickRatioGrowth number,
    ltmYOYQuickRatioGrowth number,
    lastYCQCurrentRatio number,
    ltmCurrentRatio number,
    lastLTMCurrentRatio number,
    qoqCurrentRatioGrowth number,
    yoyCurrentRatioGrowth number,
    ltmYOYCurrentRatioGrowth number,
    lastYCQAssetTurnover number,
    ltmAssetTurnover number,
    lastLTMAssetTurnover number,
    qoqAssetTurnoverGrowth number,
    yoyAssetTurnoverGrowth number,
    ltmYOYAssetTurnoverGrowth number,
    lastYCQGoodwillIntang number,
    qoqGoodwillIntangGrowth number,
    yoyGoodwillIntangGrowth number,
    lastYCQGoodwillIntangOverAssets number,
    qoqGoodwillIntangOverAssetsGrowth number,
    yoyGoodwillIntangOverAssetsGrowth number
); """

clear_dict = """DROP TABLE capStructure;"""
insertList = '''INSERT INTO capStructure(ticker, date,debt,bookValue,debtOverCap,debtOverEquity,assets,interestCoverage,quickRatio,currentRatio,assetTurnover,goodwillIntang,goodwillIntangOverAssets,lastYCQDebt,qoqDebtGrowth,yoyDebtGrowth,lastYCQBookValue,qoqBookValueGrowth,yoyBookValueGrowth,lastYCQDebtOverCap,qoqDebtOverCapGrowth,yoyDebtOverCapGrowth,lastYCQDebtOverEquity,qoqDebtOverEquityGrowth,yoyDebtOverEquityGrowth,lastYCQAssets,qoqAssetsGrowth,yoyAssetsGrowth,lastYCQInterestCoverage,ltmInterestCoverage,lastLTMInterestCoverage,qoqInterestCoverageGrowth,yoyInterestCoverageGrowth,ltmYOYInterestCoverageGrowth,lastYCQQuickRatio,ltmQuickRatio,lastLTMQuickRatio,qoqQuickRatioGrowth,yoyQuickRatioGrowth,ltmYOYQuickRatioGrowth,lastYCQCurrentRatio,ltmCurrentRatio,lastLTMCurrentRatio,qoqCurrentRatioGrowth,yoyCurrentRatioGrowth,ltmYOYCurrentRatioGrowth,lastYCQAssetTurnover,ltmAssetTurnover,lastLTMAssetTurnover,qoqAssetTurnoverGrowth,yoyAssetTurnoverGrowth,ltmYOYAssetTurnoverGrowth,lastYCQGoodwillIntang,qoqGoodwillIntangGrowth,yoyGoodwillIntangGrowth,lastYCQGoodwillIntangOverAssets,qoqGoodwillIntangOverAssetsGrowth,yoyGoodwillIntangOverAssetsGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from capStructure''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()