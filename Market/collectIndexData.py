import csv
import numpy as np

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

from datetime import datetime, date

reader = csv.reader(open("marketIndices.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
indexData = np.array(list(reader))
indexData = indexData[1:]
dates = []
VIX = []
oneYrSPY = []
fiveYrSPY = []

for row in indexData:
    if((row[0][0] == "1" and row[0][6] == "9") or row[0][5] == "9"):
        century = "19"
    else:
        century = "20"

    if(row[0][0] == "3"):
        dates.append(century + row[0][5:] + "0331")
    elif(row[0][0] == "6"):
        dates.append(century + row[0][5:] + "0630")
    elif(row[0][0] == "9"):
        dates.append(century + row[0][5:] + "0930")
    else:
        dates.append(century + row[0][6:] + "1231")
    VIX.append(float(row[1]))
    try:
        oneYrSPY.append(float(row[2]))
    except:
        oneYrSPY.append(0)
    try:
        fiveYrSPY.append(float(row[3]))
    except:
        fiveYrSPY.append(0)



import sqlite3
finished = [dates,VIX,oneYrSPY,fiveYrSPY]
finished = np.transpose(finished)
print(finished)
connection = sqlite3.connect("marketIndices.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists marketIndices (
    date varchar,
    VIX number,
    oneYrSPY number,
    fiveYrSPY number
); """

clear_dict = """DROP TABLE marketIndices;"""
insertList = '''INSERT INTO marketIndices(date, VIX,oneYrSPY, fiveYrSPY) VALUES(?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from marketIndices''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()

