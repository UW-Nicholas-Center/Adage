import csv
import numpy

reader = csv.reader(open("pricing.csv", "r", encoding="utf8", errors="ignore"), delimiter=",")
data = numpy.array(list(reader))
data = data[1:]

threeMonthPC = []
oneYearPC = []
threeYearPC = []
fiveYearPC = []
threeMonthDiv = []
oneYearDiv = []
threeYearDiv = []
fiveYearDiv = []
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
            if (consecCount >= 3):
                totalDiv = 0
                for j in range(0, 3):
                    try:
                        totalDiv += float(data[i-j][4])
                    except:
                        totalDiv += 0
                threeMonthDiv.append(totalDiv/currPrice)
            else:
                threeMonthDiv.append(0)

            if (consecCount >= 12):
                totalDiv = 0
                for j in range(0, 12):
                    try:
                        totalDiv += float(data[i-j][4])
                    except:
                        totalDiv += 0
                oneYearDiv.append(totalDiv/currPrice)
            else:
                oneYearDiv.append(0)

            if (consecCount >= 36):
                totalDiv = 0
                for j in range(0, 36):
                    try:
                        totalDiv += float(data[i-j][4])
                    except:
                        totalDiv += 0
                threeYearDiv.append(totalDiv/currPrice)
            else:
                threeYearDiv.append(0)

            if (consecCount >= 60):
                totalDiv = 0
                for j in range(0, 60):
                    try:
                        totalDiv += float(data[i-j][4])
                    except:
                        totalDiv += 0
                fiveYearDiv.append(totalDiv/currPrice)
            else:
                fiveYearDiv.append(0)


finished = [tickers, eoqDates, threeMonthDiv, oneYearDiv, threeYearDiv, fiveYearDiv]
finished = numpy.transpose(finished)

import sqlite3

connection = sqlite3.connect("pricing.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists dividends (
    ticker varchar,
    date varchar,
    threeMonthDiv number,
    oneYearDiv number,
    threeYearDiv number,
    fiveYearDiv number
); """

clear_dict = """DROP TABLE dividends;"""
insertList = """ INSERT INTO dividends(ticker, date, threeMonthDiv, oneYearDiv, threeYearDiv, fiveYearDiv) VALUES(?,?,?,?,?,?)"""

crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from dividends""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    



