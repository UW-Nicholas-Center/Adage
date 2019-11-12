# SimpleHistoryExample.py
from __future__ import print_function
from __future__ import absolute_import

import blpapi
from optparse import OptionParser
import numpy as np
import matplotlib.pyplot as plt


def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print("Connecting to %s:%s" % (options.host, options.port))
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")
        import csv
        with open('tickers.csv', 'r') as f:
            reader = csv.reader(f)
            tickers = list(reader)
        tickers = tickers[0:500]
        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        for ticker in tickers:
            request.getElement("securities").appendValue(ticker[0] + " US Equity")
        # request.getElement("fields").appendValue("EQY_FLOAT")
        # request.getElement("fields").appendValue("EQY_FREE_FLOAT_PCT")
        # request.getElement("fields").appendValue("PCT_FLT_SHARES_INSTITUTIONS")
        # request.getElement("fields").appendValue("BETA_ADJ_OVERRIDABLE")
        # request.getElement("fields").appendValue("VOLATILITY_90D")
        # request.getElement("fields").appendValue("VOLATILITY_360D")
        request.getElement("fields").appendValue("CHIEF_EXECUTIVE_OFFICER_TENURE")
        request.getElement("fields").appendValue("CHIEF_EXECUTIVE_OFFICER_AGE")
        request.getElement("fields").appendValue("CEO_FNDR_CO_FNDR_NOT_A_FNDR")
        request.getElement("fields").appendValue("FEMALE_CEO_OR_EQUIVALENT")
        request.getElement("fields").appendValue("CF_STOCK_BASED_COMPENSATION")
        request.getElement("fields").appendValue("TOT_COMPENSATION_AW_TO_EXECS")
        request.getElement("fields").appendValue("AVDR_STK_BASED_COMPENSATION_EXP")
        request.getElement("fields").appendValue("AVG_BOD_TOTAL_COMPENSATION")
        request.getElement("fields").appendValue("TOT_COMP_AW_TO_CEO_&_EQUIV")
        request.getElement("fields").appendValue("AVG_EXECUTIVE_TOT_COMPENSATION")
        request.getElement("fields").appendValue("TOT_BONUSES_PAID_TO_CEO_&_EQUIV")
        request.getElement("fields").appendValue("TOT_COMP_AW_TO_CEO_&_EQUIV")
        request.getElement("fields").appendValue("HIGHEST_BONUS_AMOUNT_PAID")
        request.getElement("fields").appendValue("PCT_WOMEN_ON_BOARD")
        request.getElement("fields").appendValue("BOARD_SIZE")
        request.getElement("fields").appendValue("BOARD_AVERAGE_TENURE")
        request.getElement("fields").appendValue("BOARD_AVERAGE_AGE")
        request.getElement("fields").appendValue("PCT_INDEPENDENT_DIRECTORS")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "QUARTERLY")
        request.set("startDate", "19900331")
        request.set("endDate", "20190630")

        print("Sending Request:", request)
        # Send the request
        session.sendRequest(request)

        tickers = []
        dates = []
        CHIEF_EXECUTIVE_OFFICER_TENURE = []
        CHIEF_EXECUTIVE_OFFICER_AGE = []
        CEO_FNDR_CO_FNDR_NOT_A_FNDR = []
        FEMALE_CEO_OR_EQUIVALENT = []
        CF_STOCK_BASED_COMPENSATION = []
        TOT_COMPENSATION_AW_TO_EXECS = []
        AVDR_STK_BASED_COMPENSATION_EXP = []
        AVG_BOD_TOTAL_COMPENSATION = []
        TOT_COMP_AW_TO_CEO_AND_EQUIV = []
        AVG_EXECUTIVE_TOT_COMPENSATION = []
        TOT_BONUSES_PAID_TO_CEO_AND_EQUIV = []
        TOT_COMP_AW_TO_CEO_AND_EQUIV = []
        HIGHEST_BONUS_AMOUNT_PAID = []
        PCT_WOMEN_ON_BOARD = []
        BOARD_SIZE = []
        BOARD_AVERAGE_TENURE = []
        BOARD_AVERAGE_AGE = []
        PCT_INDEPENDENT_DIRECTORS = []

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in ev:
                    print(msg)
                    sd = msg.getElement('securityData').getElement('fieldData').values()
                    ticker = msg.getElement('securityData').getElement('security').getValueAsString()
                    for date in sd:
                        print(date)
                        tickers.append(ticker)
                        dates.append(date.getElement('date').getValueAsString())
                        if(date.hasElement('CHIEF_EXECUTIVE_OFFICER_TENURE')):
                            CHIEF_EXECUTIVE_OFFICER_TENURE.append(date.getElement('CHIEF_EXECUTIVE_OFFICER_TENURE').getValueAsFloat())
                        else:
                            CHIEF_EXECUTIVE_OFFICER_TENURE.append(0)
                        if(date.hasElement('CHIEF_EXECUTIVE_OFFICER_AGE')):
                            CHIEF_EXECUTIVE_OFFICER_AGE.append(date.getElement('CHIEF_EXECUTIVE_OFFICER_AGE').getValueAsFloat())
                        else:
                            CHIEF_EXECUTIVE_OFFICER_AGE.append(0)
                        if(date.hasElement('CEO_FNDR_CO_FNDR_NOT_A_FNDR')):
                            CEO_FNDR_CO_FNDR_NOT_A_FNDR.append(date.getElement('CEO_FNDR_CO_FNDR_NOT_A_FNDR').getValueAsString())
                        else:
                            CEO_FNDR_CO_FNDR_NOT_A_FNDR.append(0)                                                    
                        if(date.hasElement('FEMALE_CEO_OR_EQUIVALENT')):
                            FEMALE_CEO_OR_EQUIVALENT.append(date.getElement('FEMALE_CEO_OR_EQUIVALENT').getValueAsString())
                        else:
                            FEMALE_CEO_OR_EQUIVALENT.append(0)
                        if(date.hasElement('CF_STOCK_BASED_COMPENSATION')):
                            CF_STOCK_BASED_COMPENSATION.append(date.getElement('CF_STOCK_BASED_COMPENSATION').getValueAsFloat())
                        else:
                            CF_STOCK_BASED_COMPENSATION.append(0)
                        if(date.hasElement('TOT_COMPENSATION_AW_TO_EXECS')):
                            TOT_COMPENSATION_AW_TO_EXECS.append(date.getElement('TOT_COMPENSATION_AW_TO_EXECS').getValueAsFloat())
                        else:
                            TOT_COMPENSATION_AW_TO_EXECS.append(0)
                        if(date.hasElement('AVDR_STK_BASED_COMPENSATION_EXP')):
                            AVDR_STK_BASED_COMPENSATION_EXP.append(date.getElement('AVDR_STK_BASED_COMPENSATION_EXP').getValueAsFloat())
                        else:
                            AVDR_STK_BASED_COMPENSATION_EXP.append(0)
                        if(date.hasElement('AVG_BOD_TOTAL_COMPENSATION')):
                            AVG_BOD_TOTAL_COMPENSATION.append(date.getElement('AVG_BOD_TOTAL_COMPENSATION').getValueAsFloat())
                        else:
                            AVG_BOD_TOTAL_COMPENSATION.append(0)
                        if(date.hasElement('TOT_COMP_AW_TO_CEO_&_EQUIV')):
                            TOT_COMP_AW_TO_CEO_AND_EQUIV.append(date.getElement('TOT_COMP_AW_TO_CEO_&_EQUIV').getValueAsFloat())
                        else:
                            TOT_COMP_AW_TO_CEO_AND_EQUIV.append(0)
                        if(date.hasElement('AVG_EXECUTIVE_TOT_COMPENSATION')):
                            AVG_EXECUTIVE_TOT_COMPENSATION.append(date.getElement('AVG_EXECUTIVE_TOT_COMPENSATION').getValueAsFloat())
                        else:
                            AVG_EXECUTIVE_TOT_COMPENSATION.append(0)
                        if(date.hasElement('TOT_BONUSES_PAID_TO_CEO_&_EQUIV')):
                            TOT_BONUSES_PAID_TO_CEO_AND_EQUIV.append(date.getElement('TOT_BONUSES_PAID_TO_CEO_&_EQUIV').getValueAsFloat())
                        else:
                            TOT_BONUSES_PAID_TO_CEO_AND_EQUIV.append(0)
                        if(date.hasElement('HIGHEST_BONUS_AMOUNT_PAID')):
                            HIGHEST_BONUS_AMOUNT_PAID.append(date.getElement('HIGHEST_BONUS_AMOUNT_PAID').getValueAsFloat())
                        else:
                            HIGHEST_BONUS_AMOUNT_PAID.append(0)
                        if(date.hasElement('PCT_WOMEN_ON_BOARD')):
                            PCT_WOMEN_ON_BOARD.append(date.getElement('PCT_WOMEN_ON_BOARD').getValueAsFloat())
                        else:
                            PCT_WOMEN_ON_BOARD.append(0)
                        if(date.hasElement('BOARD_SIZE')):
                            BOARD_SIZE.append(date.getElement('BOARD_SIZE').getValueAsFloat())
                        else:
                            BOARD_SIZE.append(0)
                        if(date.hasElement('BOARD_AVERAGE_TENURE')):
                            BOARD_AVERAGE_TENURE.append(date.getElement('BOARD_AVERAGE_TENURE').getValueAsFloat())
                        else:
                            BOARD_AVERAGE_TENURE.append(0)
                        if(date.hasElement('BOARD_AVERAGE_AGE')):
                            BOARD_AVERAGE_AGE.append(date.getElement('BOARD_AVERAGE_AGE').getValueAsFloat())
                        else:
                            BOARD_AVERAGE_AGE.append(0)
                        if(date.hasElement('PCT_INDEPENDENT_DIRECTORS')):
                            PCT_INDEPENDENT_DIRECTORS.append(date.getElement('PCT_INDEPENDENT_DIRECTORS').getValueAsFloat())
                        else:
                            PCT_INDEPENDENT_DIRECTORS.append(0)
                                                                                                                                                                                                                                                                                                                                                                                                                                    
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break

        for i in range(0, len(dates)):
            dates[i] = dates[i][0:4]+dates[i][5:7]+dates[i][8:10]
            if(dates[i][4:] == "1230"):
                dates[i] = dates[i][0:4] + "1231"
            if(dates[i][4:] == "0330"):
                dates[i] = dates[i][0:4] + "0331"
        finished = [tickers, dates, CHIEF_EXECUTIVE_OFFICER_TENURE,CHIEF_EXECUTIVE_OFFICER_AGE,CEO_FNDR_CO_FNDR_NOT_A_FNDR,FEMALE_CEO_OR_EQUIVALENT,CF_STOCK_BASED_COMPENSATION,AVDR_STK_BASED_COMPENSATION_EXP,AVG_BOD_TOTAL_COMPENSATION,TOT_COMP_AW_TO_CEO_AND_EQUIV,AVG_EXECUTIVE_TOT_COMPENSATION,TOT_BONUSES_PAID_TO_CEO_AND_EQUIV,HIGHEST_BONUS_AMOUNT_PAID,PCT_WOMEN_ON_BOARD,BOARD_SIZE,BOARD_AVERAGE_TENURE,BOARD_AVERAGE_AGE,PCT_INDEPENDENT_DIRECTORS]
        import sqlite3
        finished = np.transpose(finished)
        connection = sqlite3.connect("bloombergGovernance.db")
        crsr = connection.cursor()

        create_dict = """ CREATE TABLE if not exists bloombergGovernance (
            ticker varchar,
            date varchar,
            CHIEF_EXECUTIVE_OFFICER_TENURE number,
            CHIEF_EXECUTIVE_OFFICER_AGE number,
            CEO_FNDR_CO_FNDR_NOT_A_FNDR number,
            FEMALE_CEO_OR_EQUIVALENT number,
            CF_STOCK_BASED_COMPENSATION number,
            AVDR_STK_BASED_COMPENSATION_EXP number,
            AVG_BOD_TOTAL_COMPENSATION number,
            TOT_COMP_AW_TO_CEO_AND_EQUIV number,
            AVG_EXECUTIVE_TOT_COMPENSATION number,
            TOT_BONUSES_PAID_TO_CEO_AND_EQUIV number,
            HIGHEST_BONUS_AMOUNT_PAID number,
            PCT_WOMEN_ON_BOARD number,
            BOARD_SIZE number,
            BOARD_AVERAGE_TENURE number,
            BOARD_AVERAGE_AGE number,
            PCT_INDEPENDENT_DIRECTORS number
        ); """

        clear_dict = """DROP TABLE bloombergGovernance;"""
        insertList = '''INSERT INTO bloombergGovernance(ticker, date,CHIEF_EXECUTIVE_OFFICER_TENURE,CHIEF_EXECUTIVE_OFFICER_AGE,CEO_FNDR_CO_FNDR_NOT_A_FNDR,FEMALE_CEO_OR_EQUIVALENT,CF_STOCK_BASED_COMPENSATION,AVDR_STK_BASED_COMPENSATION_EXP,AVG_BOD_TOTAL_COMPENSATION,TOT_COMP_AW_TO_CEO_AND_EQUIV,AVG_EXECUTIVE_TOT_COMPENSATION,TOT_BONUSES_PAID_TO_CEO_AND_EQUIV,HIGHEST_BONUS_AMOUNT_PAID,PCT_WOMEN_ON_BOARD,BOARD_SIZE,BOARD_AVERAGE_TENURE,BOARD_AVERAGE_AGE,PCT_INDEPENDENT_DIRECTORS) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

        # crsr.execute(clear_dict)
        # print("table cleared")
        crsr.execute(create_dict)
        print("table created")
        for row in finished:
            crsr.execute(insertList,row)

        crsr.execute('''SELECT * from bloombergGovernance''')
        rows = crsr.fetchall()
        for row in rows:
            print(row)
        print("values inserted")
        print(crsr.lastrowid)
        connection.commit()



    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print("SimpleHistoryExample")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")

__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""