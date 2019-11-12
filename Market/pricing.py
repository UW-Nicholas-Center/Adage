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
    if(row[3] == currComp):
        consecCount += 1
    else:
        consecCount = 0
        currComp = row[3]

    if(row[2][4:] == "0331" or row[2][4:] == "0630" or row[2][4:] == "0930" or row[2][4:] == "1231" ):
        tickers.append(row[3])
        eoqDates.append(row[2])
        if (consecCount >= 3):
            try:
                threeMonthPC.append(float(data[i][5])/float(data[i-3][5])-1)
            except:
                threeMonthPC.append(0)
        else:
            threeMonthPC.append(0)
        if (consecCount >= 12):
            try:
                oneYearPC.append(float(data[i][5])/float(data[i-12][5])-1)
            except:
                oneYearPC.append(0)
        else:
            oneYearPC.append(0)
        if (consecCount >= 36):
            try:
                threeYearPC.append(float(data[i][5])/float(data[i-36][5])-1)
            except:
                threeYearPC.append(0)
        else:
            threeYearPC.append(0)
        if (consecCount >= 60):
            try:
                fiveYearPC.append(float(data[i][5])/float(data[i-60][5])-1)
            except:
                fiveYearPC.append(0)
        else:
            fiveYearPC.append(0)


finished = [tickers, eoqDates, threeMonthPC, oneYearPC, threeYearPC, fiveYearPC]
finished = numpy.transpose(finished)
print(finished)

import sqlite3

connection = sqlite3.connect("pricing.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists priceChange (
    ticker varchar,
    date varchar,
    threeMonthPC number,
    oneYearPC number,
    threeYearPC number,
    fiveYearPC number
); """

clear_dict = """DROP TABLE priceChange;"""
insertList = """ INSERT INTO priceChange(ticker, date, threeMonthPC, oneYearPC, threeYearPC, fiveYearPC) VALUES(?,?,?,?,?,?)"""

crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from priceChange""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    



