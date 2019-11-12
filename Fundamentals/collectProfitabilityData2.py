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


reader = csv.reader(open("ProfitabilityAndGrowth_2.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]


tickers = []
dates = []

# lastYCQ = last-year same calendar quarter

EBIT = []
lastYCQEBIT = []
ltmEBIT = []
lastLTMEBIT = []
qoqEBITGrowth = []
yoyEBITGrowth = []
ltmYoYEBITGrowth = []

EBITMargin = []
lastYCQEBITMargin = []
ltmEBITMargin = []
lastLTMEBITMargin = []
qoqEBITMarginGrowth = []
yoyEBITMarginGrowth = []
ltmYoYEBITMarginGrowth = []

NOPAT = []
lastYCQNOPAT= []
ltmNOPAT = []
lastLTMNOPAT = []
qoqNOPATGrowth = []
yoyNOPATGrowth = []
ltmYoYNOPATGrowth = []

NOPATMargin = []
lastYCQNOPATMargin= []
ltmNOPATMargin = []
lastLTMNOPATMargin = []
qoqNOPATMarginGrowth = []
yoyNOPATMarginGrowth = []
ltmYoYNOPATMarginGrowth = []

EBITDA = []
lastYCQEBITDA= []
ltmEBITDA = []
lastLTMEBITDA = []
qoqEBITDAGrowth = []
yoyEBITDAGrowth = []
ltmYoYEBITDAGrowth = []

EBITDAMargin = []
lastYCQEBITDAMargin= []
ltmEBITDAMargin = []
lastLTMEBITDAMargin = []
qoqEBITDAMarginGrowth = []
yoyEBITDAMarginGrowth = []
ltmYoYEBITDAMarginGrowth = []

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


    if(data[i][12] == ""):
        EBIT.append(1)
    else:
        EBIT.append(float(data[i][12]))
    
    if(data[i][12] == "" or data[i][14] == "" or float(data[i][14]) == 0):
        EBITMargin.append(1)
    else:
        EBITMargin.append(float(data[i][12])/float(data[i][14]))
    
    if(data[i][12] == "" or data[i][15] == ""):
        NOPAT.append(1)
    else:
        NOPAT.append(float(data[i][12])-float(data[i][15]))

    if(data[i][12] == "" or data[i][15] == "" or data[i][14] == "" or float(data[i][14]) == 0):
        NOPATMargin.append(1)
    else:
        NOPATMargin.append((float(data[i][12])-float(data[i][15]))/float(data[i][14]))

    if(data[i][13] == ""):
        EBITDA.append(1)
    else:
        EBITDA.append(float(data[i][13]))

    if(data[i][14] == "" or data[i][13] == "" or float(data[i][14]) == 0):
        EBITDAMargin.append(1)
    else:
        EBITDAMargin.append(float(data[i][13])/float(data[i][14]))


lastYCQEBIT = lastYCQ(EBIT)
ltmEBIT = ltmSum(EBIT)
lastLTMEBIT = lastLTMSum(EBIT)
qoqEBITGrowth = qoqGrowth(EBIT)
yoyEBITGrowth = yoyGrowth(EBIT)
ltmYoYEBITGrowth = ltmYoYGrowth(EBIT)
lastYCQEBITMargin = lastYCQ(EBITMargin)
ltmEBITMargin = ltmSum(EBITMargin)
lastLTMEBITMargin = lastLTMSum(EBITMargin)
qoqEBITMarginGrowth = qoqGrowth(EBITMargin)
yoyEBITMarginGrowth = yoyGrowth(EBITMargin)
ltmYoYEBITMarginGrowth = ltmYoYGrowth(EBITMargin)
lastYCQNOPAT= lastYCQ(NOPAT)
ltmNOPAT = ltmSum(NOPAT)
lastLTMNOPAT = lastLTMSum(NOPAT)
qoqNOPATGrowth = qoqGrowth(NOPAT)
yoyNOPATGrowth = yoyGrowth(NOPAT)
ltmYoYNOPATGrowth = ltmYoYGrowth(NOPAT)
lastYCQNOPATMargin= lastYCQ(NOPATMargin)
ltmNOPATMargin = ltmSum(NOPATMargin)
lastLTMNOPATMargin = lastLTMSum(NOPATMargin)
qoqNOPATMarginGrowth = qoqGrowth(NOPATMargin)
yoyNOPATMarginGrowth = yoyGrowth(NOPATMargin)
ltmYoYNOPATMarginGrowth = ltmYoYGrowth(NOPATMargin)
lastYCQEBITDA= lastYCQ(EBITDA)
ltmEBITDA = ltmSum(EBITDA)
lastLTMEBITDA = lastLTMSum(EBITDA)
qoqEBITDAGrowth = qoqGrowth(EBITDA)
yoyEBITDAGrowth = yoyGrowth(EBITDA)
ltmYoYEBITDAGrowth = ltmYoYGrowth(EBITDA)
lastYCQEBITDAMargin= lastYCQ(EBITDAMargin)
ltmEBITDAMargin = ltmSum(EBITDAMargin)
lastLTMEBITDAMargin = lastLTMSum(EBITDAMargin)
qoqEBITDAMarginGrowth = qoqGrowth(EBITDAMargin)
yoyEBITDAMarginGrowth = yoyGrowth(EBITDAMargin)
ltmYoYEBITDAMarginGrowth = ltmYoYGrowth(EBITDAMargin)


finished = [tickers,dates,EBIT,lastYCQEBIT,ltmEBIT,lastLTMEBIT,qoqEBITGrowth,yoyEBITGrowth,ltmYoYEBITGrowth,EBITMargin,lastYCQEBITMargin,ltmEBITMargin,lastLTMEBITMargin,qoqEBITMarginGrowth,yoyEBITMarginGrowth,ltmYoYEBITMarginGrowth,NOPAT,lastYCQNOPAT,ltmNOPAT,lastLTMNOPAT,qoqNOPATGrowth,yoyNOPATGrowth,ltmYoYNOPATGrowth,NOPATMargin,lastYCQNOPATMargin,ltmNOPATMargin,lastLTMNOPATMargin,qoqNOPATMarginGrowth,yoyNOPATMarginGrowth,ltmYoYNOPATMarginGrowth,EBITDA,lastYCQEBITDA,ltmEBITDA,lastLTMEBITDA,qoqEBITDAGrowth,yoyEBITDAGrowth,ltmYoYEBITDAGrowth,EBITDAMargin,lastYCQEBITDAMargin,ltmEBITDAMargin,lastLTMEBITDAMargin,qoqEBITDAMarginGrowth,yoyEBITDAMarginGrowth,ltmYoYEBITDAMarginGrowth]
finished = np.transpose(finished)

print(len(finished))
print(len(finished[0]))
import sqlite3

connection = sqlite3.connect("profitability.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists pg2 (
    ticker varchar,
    date varchar,
    EBIT number,
    lastYCQEBIT number,
    ltmEBIT number,
    lastLTMEBIT number,
    qoqEBITGrowth number,
    yoyEBITGrowth number,
    ltmYoYEBITGrowth number,
    EBITMargin number,
    lastYCQEBITMargin number,
    ltmEBITMargin number,
    lastLTMEBITMargin number,
    qoqEBITMarginGrowth number,
    yoyEBITMarginGrowth number,
    ltmYoYEBITMarginGrowth number,
    NOPAT number,
    lastYCQNOPAT number,
    ltmNOPAT number,
    lastLTMNOPAT number,
    qoqNOPATGrowth number,
    yoyNOPATGrowth number,
    ltmYoYNOPATGrowth number,
    NOPATMargin number,
    lastYCQNOPATMargin number,
    ltmNOPATMargin number,
    lastLTMNOPATMargin number,
    qoqNOPATMarginGrowth number,
    yoyNOPATMarginGrowth number,
    ltmYoYNOPATMarginGrowth number,
    EBITDA number,
    lastYCQEBITDA number,
    ltmEBITDA number,
    lastLTMEBITDA number,
    qoqEBITDAGrowth number,
    yoyEBITDAGrowth number,
    ltmYoYEBITDAGrowth number,
    EBITDAMargin number,
    lastYCQEBITDAMargin number,
    ltmEBITDAMargin number,
    lastLTMEBITDAMargin number,
    qoqEBITDAMarginGrowth number,
    yoyEBITDAMarginGrowth number,
    ltmYoYEBITDAMarginGrowth number
); """

clear_dict = """DROP TABLE pg2;"""
insertList = '''INSERT INTO pg2(ticker, date,EBIT,lastYCQEBIT,ltmEBIT,lastLTMEBIT,qoqEBITGrowth,yoyEBITGrowth,ltmYoYEBITGrowth,EBITMargin,lastYCQEBITMargin,ltmEBITMargin,lastLTMEBITMargin,qoqEBITMarginGrowth,yoyEBITMarginGrowth,ltmYoYEBITMarginGrowth,NOPAT,lastYCQNOPAT,ltmNOPAT,lastLTMNOPAT,qoqNOPATGrowth,yoyNOPATGrowth,ltmYoYNOPATGrowth,NOPATMargin,lastYCQNOPATMargin,ltmNOPATMargin,lastLTMNOPATMargin,qoqNOPATMarginGrowth,yoyNOPATMarginGrowth,ltmYoYNOPATMarginGrowth,EBITDA,lastYCQEBITDA,ltmEBITDA,lastLTMEBITDA,qoqEBITDAGrowth,yoyEBITDAGrowth,ltmYoYEBITDAGrowth,EBITDAMargin,lastYCQEBITDAMargin,ltmEBITDAMargin,lastLTMEBITDAMargin,qoqEBITDAMarginGrowth,yoyEBITDAMarginGrowth,ltmYoYEBITDAMarginGrowth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from pg2''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()