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


reader = csv.reader(open("tickers.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
tickerList = np.array(list(reader))
tickerList = tickerList[1:]
oneYearVar = []
fiveYearVar = []
sma50 = []
sma200 = []
tickers = []
dates = []

print(tickerList)
for ticker in tickerList:
    try:
        spy = yf.Ticker(ticker[0])
        marketHist = spy.history(start='1990-01-01',end='2019-09-18',interval="1d")
        marketCloses = marketHist['Close'].tolist()
        marketDates = marketHist.index
        fullLength = len(marketCloses)
        temp = []
        resetTimer = 0
        for j in range(0, len(marketCloses)):
            year = marketDates[j].strftime("%Y")
            month = marketDates[j].strftime("%m")
            day = marketDates[j].strftime("%d")
            if(j > 253 and (month == "03" or month=="06" or month=="09" or month=="12") and (day=="28" or day=="29" or day=="30" or day=="31") and resetTimer==0):
                resetTimer = 4
                tickers.append(ticker[0])
                if(month == "03"):
                    dates.append(year+"0331")
                elif(month == "06"):
                    dates.append(year+"0630")
                elif(month == "09"):
                    dates.append(year+"0930")
                elif(month == "12"):
                    dates.append(year+"1231")
                sma50.append(np.mean(marketCloses[j-49:j]))
                sma200.append(np.mean(marketCloses[j-199:j]))
            if(resetTimer > 0):
                resetTimer -= 1
    except:
        print("No data for " + ticker[0])

variances = [tickers, dates, sma50, sma200]
variances = np.transpose(variances)

import sqlite3

connection = sqlite3.connect("priceMovement.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists variance (
    ticker varchar,
    date varchar,
    sma50 number,
    sma200 number
); """

clear_dict = """DROP TABLE variance;"""
insertList = '''INSERT INTO variance(ticker, date, sma50, sma200) VALUES(?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for variance in variances:
    crsr.execute(insertList,variance)

crsr.execute('''SELECT * from variance''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()