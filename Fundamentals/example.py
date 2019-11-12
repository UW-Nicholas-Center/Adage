import csv
import numpy

reader = csv.reader(open("currentAssetsLiabilities.csv", "r", encoding="utf8", errors="ignore"), delimiter=",")
data = numpy.array(list(reader))
data = data[1:]

currentRatios = []
tickers = []
eoqDates = []

for row in data:
    if(row[12] != "" and row[13] != "" and float(row[13]) > 0):
        tickers.append(row[8])
        eoqDates.append(row[1])
        currentRatios.append(float(row[12])/float(row[13]))

finished = [tickers, eoqDates, currentRatios]
finished = numpy.transpose(finished)

import sqlite3

connection = sqlite3.connect("fundamentals.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists CurrentRatios (
    ticker varchar,
    date varchar,
    currentRatio number
); """

clear_dict = """DROP TABLE CurrentRatios;"""
insertList = """ INSERT INTO CurrentRatios(ticker, date, currentRatio) VALUES(?,?,?)"""

crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from CurrentRatios""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    



