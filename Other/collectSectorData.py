import csv
import numpy

reader = csv.reader(open("sectors.csv", "r", encoding="utf8", errors="ignore"), delimiter=",")
data = numpy.array(list(reader))
data = data[1:]

tickers = []
sectors = []
isEnergy = []
isMaterials = []
isIndustrials = []
isConsDisc = []
isConsStaples = []
isHealth = []
isFinancials = []
isIT = []
isCommServ = []
isUtil = []
isRE = []
currTicker = ""
for row in data:
    if(row[8] != currTicker):
        tickers.append(row[8])
        sectors.append(row[13][0:4])
        currTicker = row[8]
    else:
       pass

for sector in sectors:
    if(sector[0:2] == "10"):
        isEnergy.append(1)
    else:
        isEnergy.append(0)
    if(sector[0:2] == "15"):
        isMaterials.append(1)
    else:
        isMaterials.append(0)
    if(sector[0:2] == "20"):
        isIndustrials.append(1)
    else:
        isIndustrials.append(0)
    if(sector[0:2] == "25"):
        isConsDisc.append(1)
    else:
        isConsDisc.append(0)
    if(sector[0:2] == "30"):
        isConsStaples.append(1)
    else:
        isConsStaples.append(0)
    if(sector[0:2] == "35"):
        isHealth.append(1)
    else:
        isHealth.append(0)
    if(sector[0:2] == "40"):
        isFinancials.append(1)
    else:
        isFinancials.append(0)
    if(sector[0:2] == "45"):
        isIT.append(1)
    else:
        isIT.append(0)
    if(sector[0:2] == "50"):
        isCommServ.append(1)
    else:
        isCommServ.append(0)
    if(sector[0:2] == "55"):
        isUtil.append(1)
    else:
        isUtil.append(0)
    if(sector[0:2] == "60"):
        isRE.append(1)
    else:
        isRE.append(0)



finished = [tickers, isEnergy,isUtil,isRE,isMaterials,isIndustrials,isIT,isHealth,isFinancials,isConsStaples,isConsDisc]
finished = numpy.transpose(finished)

import sqlite3

connection = sqlite3.connect("sectors.db")
crsr = connection.cursor()

create_dict = """ CREATE TABLE if not exists sectors (
    ticker varchar,
    isEnergy number,
    isUtil number,
    isRE number,
    isMaterials number,
    isIndustrials number, 
    isIT number,
    isHealth number,
    isFinancials number,
    isConsStaples number,
    isConsDisc number
); """

clear_dict = """DROP TABLE sectors;"""
insertList = """ INSERT INTO sectors(ticker, isEnergy,isUtil,isRE,isMaterials,isIndustrials,isIT,isHealth,isFinancials,isConsStaples,isConsDisc) VALUES(?,?,?,?,?,?,?,?,?,?,?)"""

crsr.execute(create_dict)
for row in finished:
    crsr.execute(insertList, row)

crsr.execute("""Select * from sectors""")
rows = crsr.fetchall()
for row in rows:
    print(row)

connection.commit()


    



