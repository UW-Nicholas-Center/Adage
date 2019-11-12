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


reader = csv.reader(open("macroeconomicData.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]
fedFunds = []
treasury1Yr = []
treasury5yr = []
treasury10yr = []
treasury20yr = []
CPI = []
Libor3Month = []
PCE = []
MZM = []
hourlyWages = []
dates = []

for row in data:
    if(row[0][0:2] == "1/" or row[0][0:2] == "4/" or row[0][0:2] == "7/"  or row[0][0:2] == "10"):

        if(row[0][0:2] == "10"):
            if(row[0][5] == "9"):
                date = "19" + row[0][5:] + "0930"
            else:
                date = "20" + row[0][5:] + "0930"

        elif(row[0][0:2] == "1/"):
            if(row[0][4:] == "00"):
                date = "19991231"
            elif(row[0][4] == "9"):
                date = "19" + str(int(row[0][4:])-1) + "1231"
            else:
                if(row[0][4] == "0"):
                    date = "200" + str(int(row[0][4:])-1) + "1231"
                else:
                    date = "20" + str(int(row[0][4:])-1) + "1231"

        elif(row[0][0:2] == "4/"):
            if(row[0][4] == "9"):
                date = "19" + row[0][4:] + "0331"
            else:
                date = "20" + row[0][4:] + "0331"
        elif(row[0][0:2] == "7/"):
            if(row[0][4] == "9"):
                date = "19" + row[0][4:8] + "0630"
            else:
                date = "20" + row[0][4:8] + "0630"
        dates.append(date)
        fedFunds.append(row[1])
        treasury1Yr.append(row[2])
        treasury5yr.append(row[3])
        treasury10yr.append(row[4])
        treasury20yr.append(row[5])
        CPI.append(row[6])
        Libor3Month.append(row[7])
        PCE.append(row[8])
        MZM.append(row[9])
        hourlyWages.append(row[10])


macro = [dates, fedFunds, treasury1Yr,treasury5yr,treasury10yr,treasury20yr,CPI,Libor3Month,PCE,MZM,hourlyWages]
macro = np.transpose(macro)

import sqlite3

connection = sqlite3.connect("macro.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists macro (
    date varchar,
    fedFunds number,
    treasury1Yr number,
    treasury5Yr number,
    treasury10Yr number,
    treasury20Yr number,
    CPI number,
    Libor3Month number,
    PCE number,
    MZM number,
    hourlyWages number
); """

clear_dict = """DROP TABLE macro;"""
insertList = '''INSERT INTO macro(date, fedFunds, treasury1Yr, treasury5Yr, treasury10Yr, treasury20Yr, CPI, Libor3Month, PCE, MZM, hourlyWages) VALUES(?,?,?,?,?,?,?,?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in macro:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from macro''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()