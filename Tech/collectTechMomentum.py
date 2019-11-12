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


reader = csv.reader(open("techMomentum.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
momentumData = np.array(list(reader))

reader = csv.reader(open("internalDrift.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
driftData = np.array(list(reader))


tickers = []
dates = []
techMomentum = []
internalDrift = []

for i in range(1,len(momentumData)):
    for j in range(1, len(momentumData[i])):
        date = momentumData[i][0]
        if(date[4:] == "0331" or date[4:] == "0630" or date[4:] == "0930" or date[4:] == "1231"):
            dates.append(date)
            tickers.append(momentumData[0][j])
            techMomentum.append(momentumData[i][j])
            internalDrift.append(driftData[i][j])


finished = [tickers,dates,techMomentum,internalDrift]
finished = np.transpose(finished)

import sqlite3

connection = sqlite3.connect("tech.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists tech (
    ticker varchar,
    date varchar,
    techMomentum number,
    internalDrift number
); """

clear_dict = """DROP TABLE tech;"""
insertList = '''INSERT INTO tech(ticker,date,techMomentum,internalDrift) VALUES(?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from tech''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()