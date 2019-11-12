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
avgVolMonth = []
avgVolYear = []
volChange = []
tickers = []
dates = []

print(tickerList)
for ticker in tickerList:
    try:
        spy = yf.Ticker(ticker[0])
        marketHist = spy.history(start='1988-01-01',end='2019-09-18',interval="1d")
        marketVolume = marketHist['Volume'].tolist()
        marketDates = marketHist.index
        fullLength = len(marketVolume)
        temp = []
        resetTimer = 0
        for j in range(0, len(marketVolume)):
            year = marketDates[j].strftime("%Y")
            month = marketDates[j].strftime("%m")
            day = marketDates[j].strftime("%d")
            if(j > 253 and (month == "03" or month=="06" or month=="09" or month=="12") and (day=="28" or day=="29" or day=="30" or day=="31") and resetTimer==0):
                resetTimer = 4
                tickers.append(ticker[0])
                dates.append(year+month+day)
                volMonth = np.mean(marketVolume[j-28:j])
                volYear = np.mean(marketVolume[j-253:j])
                avgVolMonth.append(volMonth)
                avgVolYear.append(volYear)
                volChange.append(volMonth/volYear)
            if(resetTimer > 0):
                resetTimer -= 1
    except:
        print("No data for " + ticker[0])

volumes = [tickers, dates, avgVolMonth, avgVolYear, volChange]
volumes = np.transpose(volumes)

import sqlite3

connection = sqlite3.connect("volume.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists volume (
    ticker varchar,
    date varchar,
    avgVolMonth number,
    avgVolYear number,
    volChange number
); """

clear_dict = """DROP TABLE volume;"""
insertList = '''INSERT INTO volume(ticker, date, avgVolMonth, avgVolYear, volChange) VALUES(?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for volume in volumes:
    crsr.execute(insertList,volume)

crsr.execute('''SELECT * from volume''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()