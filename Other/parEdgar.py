try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import time
import csv
import sys
import time
import pandas as pd
from bs4 import BeautifulSoup
import re
import requests
import html2text
import os.path
from os import path
import asyncio
import aiohttp
import fetch


#Read in CIK CSV
ciks = pd.read_csv("ciks.csv")
ciks=ciks["CIK"].dropna()
ciks = ciks.astype(int)
print(len(ciks))
print(ciks)
#ciks = pd.to_numeric(ciks["CIK"], errors='coerce')
ciks = ciks.apply(str)
print(ciks)

# Parallelizing using Pool.apply()

import multiprocessing as mp

# Step 1: Init multiprocessing.Pool()
pool = mp.Pool(8)


#Should use the same words as in fetch.py. This doesn't actually do anything aside from being used as header in CSV
currentYear = " currentYear "
lastYear = " lastYear "
twoAgo = " twoAgo "
threeAgo = " threeAgo "
nextOne = " nextOne "
nextTwo = " nextTwo "
nextThree = " nextThree "

csvHeader = ["CIK", "Date",
             " COMPETITION ",
        " TEST ",
        " IMPROVEMENTS ",
        " UNRECOGNIZED ",
        " TESTING ",
        " ACCOUNTED ",
        " ALLOCATED ",
        " COMPETENT ",
        " STRATEGY ",
        " LONGLIVED ",
        " YIELD ",
        " NEXT ",
        " COMBINED ",
        " THEN ",
        " TRENDS ",
        " RETIREMENT ",
        " EARNED ",
        " ENVIRONMENT ",
        " PROTECTION ",
        " FINAL ",
        " INVESTING ",
        " CUMULATIVE ",
        " LOW ",
        " INVESTING ",
        " OUT ",
        " TREASURY ",
        " PERFORMED ",
        " IMPAIRED ",
        " LEASED ",
        " ASSUMED ",
        " GOODS ",
        " FASB ",
        " EARLY ",
        " ADOPTION ",
        " ENTITLED ",
        " EVALUATE ",
        " STANDARD ",
        " TRANSFER ",
        " GAAP ",
        " STRATEGIC ",
        " VARIABLE ",
        " TIMING ",
        " PROPRIETARY ",
        " NAME ",
        " RECOGNIZE ",
        " MAJOR ",
        " PROXY ",
        " ENDING ",
        " UNLESS ",
        " DEPENDENT ",
         currentYear,
             lastYear,
             twoAgo,
             threeAgo,
             nextOne,
             nextTwo,
             nextThree
                     ]




"""
  .oooooo.    oooooooooooo ooooooooooooo 
 d8P'  `Y8b   `888'     `8 8'   888   `8 
888            888              888      
888            888oooo8         888      
888     ooooo  888    "         888      
`88.    .88'   888       o      888      
 `Y8bood8P'   o888ooooood8     o888o     
                                         
                                         
                                         
ooooo     ooo ooooooooo.   ooooo         .oooooo..o 
`888'     `8' `888   `Y88. `888'        d8P'    `Y8 
 888       8   888   .d88'  888         Y88bo.      
 888       8   888ooo88P'   888          `"Y8888o.  
 888       8   888`88b.     888              `"Y88b 
 `88.    .8'   888  `88b.   888       o oo     .d8P 
   `YbodP'    o888o  o888o o888ooooood8 8""88888P'  
                                                    
                                                    """
###Go through each line of the master index file and find given CIK 
#and FILE and extract the text file path
def getURLDict(ciks, FILE, Year):
    urlDict = {}
    for QTR in range(1, 5):
        QTR = str(QTR)
        url="https://www.sec.gov/Archives/edgar/full-index/"+Year+"/QTR"+QTR+"/master.idx"
        response = urllib2.urlopen(url)
        print(response)
        string_match1 = 'edgar/data/'
        element2 = None
        element3 = None
        element4 = None

        for line in response:
            try:
                line = line.decode("utf-8")
                line = line.split("|")
            except:
                continue
            try:
                print(line)
                if (line[2] in FILE):
                    #URLdict[CIK]=[Date, URL]
                    urlDict[line[0]] = [line[3],'https://www.sec.gov/Archives/'+(line[4])[:-1]]
            except:
                continue

    return urlDict  


"""
  .oooooo.    oooooooooooo ooooooooooooo 
 d8P'  `Y8b   `888'     `8 8'   888   `8 
888            888              888      
888            888oooo8         888      
888     ooooo  888    "         888      
`88.    .88'   888       o      888      
 `Y8bood8P'   o888ooooood8     o888o     
                                         
                                         
                                         
ooooooooooooo oooooooooooo ooooooo  ooooo ooooooooooooo 
8'   888   `8 `888'     `8  `8888    d8'  8'   888   `8 
     888       888            Y888..8P         888      
     888       888oooo8        `8888'          888      
     888       888    "       .8PY888.         888      
     888       888       o   d8'  `888b        888      
    o888o     o888ooooood8 o888o  o88888o     o888o     
                                                      """
def getRequests(ciks):
    saveTime = time.time()
    requestDict = {}
    for CIK in ciks:
        cikStart = time.time()
        urls = []
        urlYears = []
        for Year in range(1994, 2019):
            currentYear = str(" "+ str(Year)+ " ")
            lastYear = str(" "+str(Year-1)+ " ")
            twoAgo = str(" "+str(Year-2)+" ")
            threeAgo = str(" "+str(Year-3)+ " ")
            nextOne = str(" "+str(Year+1)+" ")
            nextTwo = str(" "+str(Year+2)+" ")
            nextThree = str(" "+str(Year+3)+ " ")
            Year = str(Year)
            urlDictName = Year+"Newdict.csv"
            if path.exists(urlDictName):
                with open(urlDictName) as f:
                    next(f)  # Skip the header
                    reader = csv.reader(f, skipinitialspace=True)
                    urlDict = dict(reader)

            else:
                urlDict = getURLDict(ciks, FILE, Year)
                with open(urlDictName, 'w') as csv_file:
                    writer = csv.writer(csv_file)
                    for key, value in urlDict.items():
                        writer.writerow([key, value])
            Year = int(Year)
            csvHeader[52] = str(" "+ str(Year)+ " ")
            csvHeader[53] = str(" "+ str(Year-1)+ " ")
            csvHeader[54] = str(" "+ str(Year-2)+ " ")
            csvHeader[55] = str(" "+ str(Year-3)+ " ")
            csvHeader[56] = str(" "+ str(Year+1)+ " ")
            csvHeader[57] = str(" "+ str(Year+2)+ " ")
            csvHeader[58] = str(" "+ str(Year+3)+ " ")
            Year=str(Year)
            if CIK in urlDict:
                cikArray = urlDict[CIK].split(",",1)
                cikURL = (cikArray[1])
                cikURL = cikURL[2:-2]
                cikDate = cikArray[0]
                cikDate = cikDate[2:-1]
                print(cikDate)
                print(cikURL)
                urls.append([cikURL, csvHeader])
                urlYears.append(cikDate)
                print(urlYears)
                #print("URL: ", urls)
            else:
                continue
                
        try:
            text = pool.map(fetch.f, urls)
            print(len(text))
            print(len(urlYears))
            newText = []
            newDates = []
            for i in range(0, len(text)):
                if text[i]!=None:
                    newText.append(text[i])
                    newDates.append(urlYears[i])
            text = newText
            urlYears = newDates
            print(len(text))
            print(len(urlYears))
            print(text)
            print(urlYears)
            requestDict[CIK] = {}
            for i in range(0, len(text)):
                requestDict[CIK][urlYears[i]] = text[i]
            print(requestDict[CIK])
            print("CIK runtime: ", time.time()-cikStart)
            if (time.time()-saveTime) > 180:
                print("SAVING")
                saveTime = time.time()
                with open("mdaDataPt1.csv","w") as f:
                    csvWriter = csv.writer(f,delimiter=',')
                    csvWriter.writerow(csvHeader)
                    for cik in rDict:
                        for year in rDict[cik]:
                            counts = rDict[cik][year]
                            counts.insert(0,year)
                            counts.insert(0, cik)
                        #csvWriter.writerow(words) 
            
                            csvWriter.writerow(counts)
                f.close()

        except Exception as e:
            print(e)
            time.sleep(1)
            continue


    return requestDict




    
    
    
    
       
#CIK = '1018724' #AMAZON COM
#Year = '2014' 
FILE='10-K'
#Set quarter. Only applicable to quarterly documents. 
#QTR = "2"
start = time.time()
companiesData = []
rDict = getRequests(ciks[5000:])


#Tidy up CSV headers


with open("mdaDataPt3.csv","w") as f:
    csvWriter = csv.writer(f,delimiter=',')
    csvWriter.writerow(csvHeader)
    for cik in rDict:
        for year in rDict[cik]:
            counts = rDict[cik][year]
            counts.insert(0,year)
            counts.insert(0, cik)
        #csvWriter.writerow(words) 
            
            csvWriter.writerow(counts)
f.close()

print(rDict)
print("Total time: ", time.time()-start)