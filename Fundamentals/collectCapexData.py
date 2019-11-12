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


reader = csv.reader(open("capex_and_noncontrollinginterest.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

CapexOverSales = []
lastYCQCapexOverSales = []
ltmCapexOverSales = []
lastLTMCapexOverSales = []
qoqCapexOverSalesGrowth = []
yoyCapexOverSalesGrowth = []
ltmYoYCapexOverSalesGrowth = []

Capex = []
lastYCQCapex = []
ltmCapex = []
lastLTMCapex = []
qoqCapexGrowth = []
yoyCapexGrowth = []
ltmYoYCapexGrowth = []

NCInterests = []
lastYCQNCInterests= []
ltmNCInterests = []
lastLTMNCInterests = []
qoqNCInterestsGrowth = []
yoyNCInterestsGrowth = []
ltmYoYNCInterestsGrowth = []

CapexOverPPE = []
lastYCQCapexOverPPE= []
ltmCapexOverPPE = []
lastLTMCapexOverPPE = []
qoqCapexOverPPEGrowth = []
yoyCapexOverPPEGrowth = []
ltmYoYCapexOverPPEGrowth = []


errorNum = 0
missingNum = 1.23456

for i in range(0, len(data)):
    if(data[i][16] == ""):
        currCapex = missingNum
    else:
        currCapex = float(data[i][16])/float(data[i][3])
    Capex.append(currCapex)

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


    if(data[i][15] == "" or float(data[i][15]) == 0 or currCapex == missingNum):
        CapexOverSales.append(missingNum)
    else:
        CapexOverSales.append(currCapex/float(data[i][15]))
    
    if(data[i][14] == "" or float(data[i][14]) == 0 or currCapex == missingNum):
        CapexOverPPE.append(missingNum)
    else:
        CapexOverPPE.append(currCapex/float(data[i][14]))
    
    if(data[i][12] == ""):
        NCInterests.append(0)
    else:
        NCInterests.append(float(data[i][12]))




lastYCQCapexOverSales = lastYCQ(CapexOverSales)
ltmCapexOverSales = ltmSum(CapexOverSales)
lastLTMCapexOverSales = lastLTMSum(CapexOverSales)
qoqCapexOverSalesGrowth = qoqGrowth(CapexOverSales)
yoyCapexOverSalesGrowth = yoyGrowth(CapexOverSales)
ltmYoYCapexOverSalesGrowth = ltmYoYGrowth(CapexOverSales)
lastYCQCapex = lastYCQ(Capex)
ltmCapex = ltmSum(Capex)
lastLTMCapex = lastLTMSum(Capex)
qoqCapexGrowth = qoqGrowth(Capex)
yoyCapexGrowth = yoyGrowth(Capex)
ltmYoYCapexGrowth = ltmYoYGrowth(Capex)
lastYCQNCInterests= lastYCQ(NCInterests)
ltmNCInterests = ltmSum(NCInterests)
lastLTMNCInterests = lastLTMSum(NCInterests)
qoqNCInterestsGrowth = qoqGrowth(NCInterests)
yoyNCInterestsGrowth = yoyGrowth(NCInterests)
ltmYoYNCInterestsGrowth = ltmYoYGrowth(NCInterests)
lastYCQCapexOverPPE= lastYCQ(CapexOverPPE)
ltmCapexOverPPE = ltmSum(CapexOverPPE)
lastLTMCapexOverPPE = lastLTMSum(CapexOverPPE)
qoqCapexOverPPEGrowth = qoqGrowth(CapexOverPPE)
yoyCapexOverPPEGrowth = yoyGrowth(CapexOverPPE)
ltmYoYCapexOverPPEGrowth = ltmYoYGrowth(CapexOverPPE)



finished = [tickers,dates,CapexOverSales,lastYCQCapexOverSales,ltmCapexOverSales,lastLTMCapexOverSales,qoqCapexOverSalesGrowth,yoyCapexOverSalesGrowth,ltmYoYCapexOverSalesGrowth,Capex,lastYCQCapex,ltmCapex,lastLTMCapex,qoqCapexGrowth,yoyCapexGrowth,ltmYoYCapexGrowth,NCInterests,lastYCQNCInterests,ltmNCInterests,lastLTMNCInterests,qoqNCInterestsGrowth,yoyNCInterestsGrowth,ltmYoYNCInterestsGrowth,CapexOverPPE,lastYCQCapexOverPPE,ltmCapexOverPPE,lastLTMCapexOverPPE,qoqCapexOverPPEGrowth,yoyCapexOverPPEGrowth,ltmYoYCapexOverPPEGrowth]
finished = np.transpose(finished)

print(len(finished))
print(len(finished[0]))
import sqlite3

connection = sqlite3.connect("capexAndNCInterests.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists capexAndNCInterests (
    ticker varchar,
    date varchar,
    CapexOverSales number,
    lastYCQCapexOverSales number,
    ltmCapexOverSales number,
    lastLTMCapexOverSales number,
    qoqCapexOverSalesGrowth number,
    yoyCapexOverSalesGrowth number,
    ltmYoYCapexOverSalesGrowth number,
    Capex number,
    lastYCQCapex number,
    ltmCapex number,
    lastLTMCapex number,
    qoqCapexGrowth number,
    yoyCapexGrowth number,
    ltmYoYCapexGrowth number,
    NCInterests number,
    lastYCQNCInterests number,
    ltmNCInterests number,
    lastLTMNCInterests number,
    qoqNCInterestsGrowth number,
    yoyNCInterestsGrowth number,
    ltmYoYNCInterestsGrowth number,
    CapexOverPPE number,
    lastYCQCapexOverPPE number,
    ltmCapexOverPPE number,
    lastLTMCapexOverPPE number,
    qoqCapexOverPPEGrowth number,
    yoyCapexOverPPEGrowth number,
    ltmYoYCapexOverPPEGrowth number
); """

clear_dict = """DROP TABLE capexAndNCInterests;"""
insertList = '''INSERT INTO capexAndNCInterests(ticker, date,CapexOverSales,lastYCQCapexOverSales,ltmCapexOverSales,lastLTMCapexOverSales,qoqCapexOverSalesGrowth,yoyCapexOverSalesGrowth,ltmYoYCapexOverSalesGrowth,Capex,lastYCQCapex,ltmCapex,lastLTMCapex,qoqCapexGrowth,yoyCapexGrowth,ltmYoYCapexGrowth,NCInterests,lastYCQNCInterests,ltmNCInterests,lastLTMNCInterests,qoqNCInterestsGrowth,yoyNCInterestsGrowth,ltmYoYNCInterestsGrowth,CapexOverPPE,lastYCQCapexOverPPE,ltmCapexOverPPE,lastLTMCapexOverPPE,qoqCapexOverPPEGrowth,yoyCapexOverPPEGrowth,ltmYoYCapexOverPPEGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from capexAndNCInterests''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()