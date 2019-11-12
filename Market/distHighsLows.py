import csv
import numpy

reader = csv.reader(open("pricing.csv", "r", encoding="utf8", errors="ignore"), delimiter=",")
data = numpy.array(list(reader))
data = data[1:]

pctYrHigh = []
pctYrLow = []
pct3YrHigh = []
pct3YrLow = []
tickers = []
eoqDates = []

currComp = ""
consecCount = 0
print(len(data))
for i in range(0, len(data)):
    row = data[i]
    currPrice = row[5]
    if(row[3] == currComp):
        consecCount += 1
    else:
        consecCount = 0
        currComp = row[3]

    if(currPrice != ""):
        currPrice = float(currPrice)
        if(row[2][4:] == "0331" or row[2][4:] == "0630" or row[2][4:] == "0930" or row[2][4:] == "1231" ):
            tickers.append(row[3])
            eoqDates.append(row[2])
            if (consecCount >= 12):
                YrHigh = 0
                YrLow = 10000000
                for j in range(0, 12):
                    try:
                        if(float(data[i-j][6]) > YrHigh):
                            YrHigh = float(data[i-j][6])
                        if(float(data[i-j][7]) != 0 and float(data[i-j][7]) < YrLow):
                            YrLow = float(data[i-j][7])
                    except:
                        pass
                if(YrHigh == 0):
                    pctYrHigh.append(0)
                else:
                    pctYrHigh.append(currPrice/YrHigh)
                if(YrLow == 0):
                    pctYrLow.append(0)
                else:
                    pctYrLow.append(currPrice/YrLow)
            else:
                pctYrHigh.append(0)
                pctYrLow.append(0)

            if (consecCount >= 36):
                threeYrHigh = 0
                threeYrLow = 1000000000000
                for j in range(0, 36):
                    try:
                        if(float(data[i-j][6]) > threeYrHigh):
                            threeYrHigh = float(data[i-j][6])
                        if(float(data[i-j][7]) != 0 and float(data[i-j][7]) < threeYrLow):
                            threeYrLow = float(data[i-j][7])
                    except:
                        pass
                if(threeYrHigh == 0):
                    pct3YrHigh.append(0)
                else:
                    pct3YrHigh.append(currPrice/threeYrHigh)
                if(threeYrLow == 0):
                    pct3YrLow.append(0)
                else:
                    pct3YrLow.append(currPrice/threeYrLow)
            else:
                pct3YrHigh.append(0)
                pct3YrLow.append(0)



finished = [tickers, eoqDates, pctYrHigh, pctYrLow, pct3YrHigh, pct3YrLow]
finished = numpy.transpose(finished)

import sqlite3

connection = sqlite3.connect("pricing.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists highLows (
    ticker varchar,
    date varchar,
    pctYrHigh number,
    pctYrLow number,
    pct3YrHigh number,
    pct3YrLow number
); """

clear_dict = """DROP TABLE highLows;"""
insertList = """ INSERT INTO highLows(ticker, date, pctYrHigh, pctYrLow, pct3YrHigh, pct3YrLow) VALUES(?,?,?,?,?,?)"""

crsr.execute(clear_dict)
crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from highLows""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    