import csv
import numpy as np

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

from datetime import datetime, date

reader = csv.reader(open("acquisitions.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
data = np.array(list(reader))
data = data[1:]
dates = []
acquisitions = []
for i in range(1980,2019):
    dates.append(str(i) + "0331")
    dates.append(str(i) + "0630")
    dates.append(str(i) + "0930")
    dates.append(str(i) + "1231")
    for j in range(0,4):
        acquisitions.append(0)
print(len(dates))
print(len(acquisitions))

for row in data:
    origDate = row[1]
    year = origDate[:4]
    month = origDate[4:6]
    day = origDate[6:]
    if(int(month) < 4):
        month = "03"
        day = "31"
    elif(int(month) < 7):
        month = "06"
        day = "30"
    elif(int(month) < 10):
        month = "09"
        day = "30"
    else:
        month = "12"
        day = "31"
    newDate = year+month+day
    print(newDate)
    for i in range(0, len(dates)):
        if(newDate == dates[i]):
            acquisitions[i] += 1

import sqlite3
finished = [dates,acquisitions]
finished = np.transpose(finished)
print(finished)
connection = sqlite3.connect("marketAcquisitions.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists marketAcquisitions (
    date varchar,
    marketAcquisitions number
); """

clear_dict = """DROP TABLE marketAcquisitions;"""
insertList = '''INSERT INTO marketAcquisitions(date, marketAcquisitions) VALUES(?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from marketAcquisitions''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()

