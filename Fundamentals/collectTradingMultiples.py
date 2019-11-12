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


reader = csv.reader(open("tradingMultiples2.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]

tickers = []
dates = []

# lastYCQ = last-year same calendar quarter


MarketCap = []
lastYCQMarketCap = []
qoqMarketCapGrowth = []
yoyMarketCapGrowth = []

MarketCaptoBVOE = []
lastYCQMarketCaptoBVOE= []
qoqMarketCaptoBVOEGrowth = []
yoyMarketCaptoBVOEGrowth = []

EVtoEBITDA = []
lastYCQEVtoEBITDA= []
qoqEVtoEBITDAGrowth = []
yoyEVtoEBITDAGrowth = []

SharePriceToEPS = []
lastYCQSharePriceToEPS= []
qoqSharePriceToEPSGrowth = []
yoySharePriceToEPSGrowth = []

EV = []
lastYCQEV= []
qoqEVGrowth = []
yoyEVGrowth = []


errorNum = 0
missingNum = 1.23456

ebitda = []

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

    
    if(data[i][14] == "" or data[i][25] == ""):
        MarketCap.append(missingNum)
    else:
        MarketCap.append(float(data[i][25])*float(data[i][14]))

    if(data[i][15] == ""):
        ltDebt = 0
    else:
        ltDebt = float(data[i][15])
    if(data[i][16] == ""):
        stDebt = 0
    else:
        stDebt = float(data[i][16])
    if(data[i][13] == ""):
        cash = 0
    else:
        cash = float(data[i][13])
    if(MarketCap[i] == missingNum):
        EV.append(missingNum)
    else:
        EV.append(MarketCap[i]+ltDebt+stDebt-cash)
    
    if(MarketCap[i] == missingNum or data[i][20] == "" or float(data[i][20]) == 0):
        MarketCaptoBVOE.append(missingNum)
    else:
        MarketCaptoBVOE.append(MarketCap[i]/float(data[i][20]))

    if(data[i][17] == "" or data[i][25] == "" or float(data[i][17]) == 0):
        SharePriceToEPS.append(missingNum)
    else:
        SharePriceToEPS.append(float(data[i][25])/float(data[i][17]))

for i in range(0, len(data)):
    if(data[i][21] == ""):
        taxes = 0
    else:
        taxes = float(data[i][21])
    if(data[i][22] == ""):
        interestExp = 0
    else:
        interestExp = float(data[i][22])
    if(data[i][26] == ""):
        depreciation = 0
    else:
        depreciation = float(data[i][26])
    if(data[i][19] == ""):
        ebitda.append(missingNum)
    else:
        ebitda.append(float(data[i][19]) + taxes + interestExp + depreciation)
ebitda = ltmSum(ebitda)

for i in range(0, len(data)):
    if(MarketCap[i] == missingNum or ebitda[i] == missingNum or ebitda[i] == 0):
        EVtoEBITDA.append(missingNum)
    else:
        EVtoEBITDA.append(MarketCap[i]/ebitda[i])

lastYCQMarketCap = lastYCQ(MarketCap)
qoqMarketCapGrowth = qoqGrowth(MarketCap)
yoyMarketCapGrowth = yoyGrowth(MarketCap)
lastYCQMarketCaptoBVOE= lastYCQ(MarketCaptoBVOE)
qoqMarketCaptoBVOEGrowth = qoqGrowth(MarketCaptoBVOE)
yoyMarketCaptoBVOEGrowth = yoyGrowth(MarketCaptoBVOE)
lastYCQEVtoEBITDA= lastYCQ(EVtoEBITDA)
qoqEVtoEBITDAGrowth = qoqGrowth(EVtoEBITDA)
yoyEVtoEBITDAGrowth = yoyGrowth(EVtoEBITDA)
lastYCQSharePriceToEPS= lastYCQ(SharePriceToEPS)
qoqSharePriceToEPSGrowth = qoqGrowth(SharePriceToEPS)
yoySharePriceToEPSGrowth = yoyGrowth(SharePriceToEPS)
lastYCQEV= lastYCQ(EV)
qoqEVGrowth = qoqGrowth(EV)
yoyEVGrowth = yoyGrowth(EV)


finished = [tickers,dates,MarketCap,lastYCQMarketCap,qoqMarketCapGrowth,yoyMarketCapGrowth,MarketCaptoBVOE,lastYCQMarketCaptoBVOE,qoqMarketCaptoBVOEGrowth,yoyMarketCaptoBVOEGrowth,EVtoEBITDA,lastYCQEVtoEBITDA,qoqEVtoEBITDAGrowth,yoyEVtoEBITDAGrowth,SharePriceToEPS,lastYCQSharePriceToEPS,qoqSharePriceToEPSGrowth,yoySharePriceToEPSGrowth,EV,lastYCQEV,qoqEVGrowth,yoyEVGrowth]

finished = np.transpose(finished)

print(len(finished))
print(len(finished[0]))
import sqlite3

connection = sqlite3.connect("tradingMultiples.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists tm1 (
    ticker varchar,
    date varchar,
    MarketCap number,
    lastYCQMarketCap number,
    qoqMarketCapGrowth number,
    yoyMarketCapGrowth number,
    MarketCaptoBVOE number,
    lastYCQMarketCaptoBVOE number,
    qoqMarketCaptoBVOEGrowth number,
    yoyMarketCaptoBVOEGrowth number,
    EVtoEBITDA number,
    lastYCQEVtoEBITDA number,
    qoqEVtoEBITDAGrowth number,
    yoyEVtoEBITDAGrowth number,
    SharePriceToEPS number,
    lastYCQSharePriceToEPS number,
    qoqSharePriceToEPSGrowth number,
    yoySharePriceToEPSGrowth number,
    EV number,
    lastYCQEV number,
    qoqEVGrowth number,
    yoyEVGrowth number
); """

clear_dict = """DROP TABLE tm1;"""
insertList = '''INSERT INTO tm1(ticker, date,MarketCap,lastYCQMarketCap,qoqMarketCapGrowth,yoyMarketCapGrowth,MarketCaptoBVOE,lastYCQMarketCaptoBVOE,qoqMarketCaptoBVOEGrowth,yoyMarketCaptoBVOEGrowth,EVtoEBITDA,lastYCQEVtoEBITDA,qoqEVtoEBITDAGrowth,yoyEVtoEBITDAGrowth,SharePriceToEPS,lastYCQSharePriceToEPS,qoqSharePriceToEPSGrowth,yoySharePriceToEPSGrowth,EV,lastYCQEV,qoqEVGrowth,yoyEVGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from tm1''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()