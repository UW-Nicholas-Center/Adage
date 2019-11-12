import csv
import numpy

reader = csv.reader(open("options.csv", "r", encoding="utf8", errors="ignore"), delimiter=",")
data = numpy.array(list(reader))
data = data[1:]

optionsValue = []
pctOptionsGranted = []
tickers = []
eoqDates = []
currTicker = "AIR"
currYear = "1996"
cumOptVal = 0
cumPctOptGranted = 0
for row in data:
    if(row[6] == currTicker and row[4] == currYear):
        try:
            cumOptVal += float(row[2])
            cumPctOptGranted += float(row[1])
        except:
            pass
    else:
        eoqDates.append(currYear + "0331")
        eoqDates.append(currYear + "0630")
        eoqDates.append(currYear + "0930")
        eoqDates.append(currYear + "1231")
        for i in range(0,4):
            tickers.append(currTicker)
            pctOptionsGranted.append(cumPctOptGranted)
            optionsValue.append(cumOptVal)
        currTicker = row[6]
        currYear = row[4]
        try:
            cumOptVal = float(row[2])
            cumPctOptGranted = float(row[1])
        except:
            cumOptVal = 0
            cumPctOptGranted = 0



finished = [tickers, eoqDates, optionsValue, pctOptionsGranted]
finished = numpy.transpose(finished)

import sqlite3

connection = sqlite3.connect("options.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists options (
    ticker varchar,
    date varchar,
    optionsValue number,
    pctOptionsGranted number
); """

clear_dict = """DROP TABLE options;"""
insertList = """ INSERT INTO options(ticker, date, optionsValue,pctOptionsGranted) VALUES(?,?,?,?)"""

crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from options""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    



