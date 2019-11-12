import csv
import numpy as np

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

from datetime import datetime, date

reader = csv.reader(open("acquisitions.csv", "r",encoding="utf8", errors='ignore'), delimiter=",")
acquisitions = np.array(list(reader))
acquisitions = acquisitions[1:]
dates = []
tickers = []
tvEBITDA = []
acquired = []
errors = 0
for acquisition in acquisitions:
    if(float(acquisition[1][4:6]) >= 10):
        dates.append(acquisition[1][0:4] + "0930")
        dates.append(acquisition[1][0:4] + "0630")
        dates.append(acquisition[1][0:4] + "0331")
        dates.append(str(int(acquisition[1][0:4])-1) + "1231")
    elif(float(acquisition[1][4:6]) >= 7):
        dates.append(acquisition[1][0:4] + "0630")
        dates.append(acquisition[1][0:4] + "0331")
        dates.append(str(int(acquisition[1][0:4])-1) + "1231")
        dates.append(str(int(acquisition[1][0:4])-1) + "0930")
    elif(float(acquisition[1][4:6]) >= 4):
        dates.append(acquisition[1][0:4] + "0331")
        dates.append(str(int(acquisition[1][0:4])-1) + "1231")
        dates.append(str(int(acquisition[1][0:4])-1) + "0930")
        dates.append(str(int(acquisition[1][0:4])-1) + "0630")
    else:
        dates.append(str(int(acquisition[1][0:4])-1) + "1231")
        dates.append(str(int(acquisition[1][0:4])-1) + "0930")
        dates.append(str(int(acquisition[1][0:4])-1) + "0630")
        dates.append(str(int(acquisition[1][0:4])-1) + "0331")
    for i in range(0,4):
        tickers.append(acquisition[6])
        try:
            tvEBITDA.append(float(acquisition[4]))
        except:
            tvEBITDA.append(0)
        acquired.append(1)
    # except:
    #     errors += 1
    #     print("ERROR")
    #     pass


import sqlite3
finished = [dates,tickers,tvEBITDA,acquired]
finished = np.transpose(finished)
print(finished)
connection = sqlite3.connect("acquisitions.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists acquisitions (
    ticker varchar,
    date varchar,
    tvEBITDA number,
    acquired number
); """

clear_dict = """DROP TABLE acquisitions;"""
insertList = '''INSERT INTO acquisitions(ticker, date,tvEBITDA, acquired) VALUES(?,?,?,?)'''

# crsr.execute(clear_dict)
# print("table cleared")
crsr.execute(create_dict)
print("table created")
for row in finished:
    crsr.execute(insertList,row)

crsr.execute('''SELECT * from acquisitions''')
rows = crsr.fetchall()
for row in rows:
    print(row)
print("values inserted")
print(crsr.lastrowid)
connection.commit()

