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


reader = csv.reader(open("topline.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]
quarterRevenue = []
lastCQRevenue = []
LTMRevenue = []
lastLTMRevenue = []
qoqRevGrowth = []
yoyRevGrowth = []
ltmYOYRevGrowth = []
qoqThreeYrCAGR = []
yoyThreeYrCAGR = []
ltmYOYThreeYrCAGR = []
qoqFiveYrCAGR = []
yoyFiveYrCAGR = []
ltmYOYFiveYrCAGR = []

tickers = []
dates = []

errorNum = 0

for i in range(0, len(data)):
    tickers.append(data[i][8])
    year = data[i][1][0:4]
    month = float(data[i][1][4:6])

    fqtr = float(data[i][11][5:])

    if(month < 4):
        dates.append("" + year + "0331")
    elif(month < 7):
        dates.append("" + year + "0630")
    elif(month < 10):
        dates.append("" + year + "0930")
    else:
        dates.append("" + year + "1231")

    if(data[i][12] == "" or float(data[i][12]) == 0):
        quarterRevenue.append(1)
    else:
        quarterRevenue.append(float(data[i][12]))

    if(i > 1 and data[i][8] == data[i-1][8] and quarterRevenue[i-1] != 1):
        qoqRevGrowth.append(quarterRevenue[i]/quarterRevenue[i-1]-1)
    else:
        qoqRevGrowth.append(0)

    if(i > 4 and data[i][8] == data[i-4][8]):
        try:
            lastCQRevenue.append(float(data[i-4][12]))
        except:
            lastCQRevenue.append(0)
        temp = 0
        for j in range(0,4):
            temp += quarterRevenue[i-j]
        LTMRevenue.append(temp)
        lastLTMRevenue.append(LTMRevenue[i-4])
        if(quarterRevenue[i-4]!= 1):
            yoyRevGrowth.append(quarterRevenue[i]/quarterRevenue[i-4]-1)
        else:
            yoyRevGrowth.append(0)
        ltmYOYThreeYrCAGR.append(yoyThreeYrCAGR[i-1])
        ltmYOYFiveYrCAGR.append(yoyFiveYrCAGR[i-1])
    else:
        lastCQRevenue.append(1)
        LTMRevenue.append(1)
        lastLTMRevenue.append(1)
        yoyRevGrowth.append(1)
        ltmYOYThreeYrCAGR.append(1)
        ltmYOYFiveYrCAGR.append(1)

    if(i > 8 and data[i][8] == data[i-8][8] and LTMRevenue[i-4] != 1):
        ltmYOYRevGrowth.append(LTMRevenue[i]/LTMRevenue[i-4]-1)
    else:
        ltmYOYRevGrowth.append(0)
    
    if(i>12 and data[i][8] == data[i-12][8] and quarterRevenue[i-12] != 1):
        qoqThreeYrCAGR.append((quarterRevenue[i]/quarterRevenue[i-12]) ** (1.0/3) - 1)
    else:
        qoqThreeYrCAGR.append(0)
    
    if(i>16 and data[i][8] == data[i-16][8] and LTMRevenue[i-12]!= 1):
        yoyThreeYrCAGR.append((LTMRevenue[i]/LTMRevenue[i-12]) ** (1.0/3) - 1)
    else:
        yoyThreeYrCAGR.append(0)

    if(i>20 and data[i][8] == data[i-20][8] and quarterRevenue[i-20]!=1):
        qoqFiveYrCAGR.append((quarterRevenue[i]/quarterRevenue[i-20]) ** (1.0/3) - 1)
    else:
        qoqFiveYrCAGR.append(0)
    
    if(i>24 and data[i][8] == data[i-24][8] and LTMRevenue[i-20]!=1):
        yoyFiveYrCAGR.append((LTMRevenue[i]/LTMRevenue[i-20]) ** (1.0/3) - 1)
    else:
        yoyFiveYrCAGR.append(0)




    

finished = [tickers, dates, quarterRevenue, lastCQRevenue,LTMRevenue,lastLTMRevenue,qoqRevGrowth,yoyRevGrowth,ltmYOYRevGrowth,qoqThreeYrCAGR,yoyThreeYrCAGR,ltmYOYThreeYrCAGR,qoqFiveYrCAGR,yoyFiveYrCAGR,ltmYOYFiveYrCAGR]
finished = np.transpose(finished)

import sqlite3

connection = sqlite3.connect("topline.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists topline (
    ticker varchar,
    date varchar,
    quarterRev number,
    lastCQRev number,
    LTMRev number,
    lastLTMRev number,
    qoqRevGrowth number,
    yoyRevGrowth number,
    ltmYOYRevGrowth number,
    qoqThreeYrCAGR number,
    yoyThreeYrCAGR number,
    ltmYOYThreeYrCAGR number,
    qoqFiveYrCAGR number,
    yoyFiveYrCAGR number,
    ltmYOYFiveYrCAGR number
); """

clear_dict = """DROP TABLE topline;"""
insertList = '''INSERT INTO topline(ticker, date, quarterRev,lastCQRev,LTMRev,lastLTMRev,qoqRevGrowth,yoyRevGrowth,ltmYOYRevGrowth,qoqThreeYrCAGR,yoyThreeYrCAGR,ltmYOYThreeYrCAGR,qoqFiveYrCAGR,yoyFiveYrCAGR,ltmYOYFiveYrCAGR) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from topline''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()