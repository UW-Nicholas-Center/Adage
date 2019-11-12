import csv
import numpy as np

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

from datetime import datetime, date

reader = csv.reader(open("taxRates.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
taxRates = np.array(list(reader))
taxRates = taxRates[1:]
dates = []
usRate = []
ukRate = []
chinaRate = []
for row in taxRates:
    dates.append(row[0][6:10] + "0930")
    dates.append(row[0][6:10] + "0630")
    dates.append(row[0][6:10] + "0331")
    dates.append(row[0][6:10] + "1231")
    for i in range(0,4):
        usRate.append(float(row[1]))
        ukRate.append(float(row[3]))
        chinaRate.append(float(row[2]))



import sqlite3
finished = [dates,usRate,ukRate,chinaRate]
finished = np.transpose(finished)
print(finished)
connection = sqlite3.connect("taxRates.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists taxRates (
    date varchar,
    usRate number,
    ukRate number,
    chinaRate number
); """

clear_dict = """DROP TABLE taxRates;"""
insertList = '''INSERT INTO taxRates(date, usRate,ukRate, chinaRate) VALUES(?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from taxRates''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()

