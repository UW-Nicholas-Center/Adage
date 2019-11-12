# SimpleHistoryExample.py
from __future__ import print_function
from __future__ import absolute_import

import blpapi
from optparse import OptionParser
import numpy as np



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
        tickers = tickers[0:1000]
        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        for ticker in tickers:
            request.getElement("securities").appendValue(ticker[0] + " US Equity")
        request.getElement("fields").appendValue("EQY_FLOAT")
        request.getElement("fields").appendValue("EQY_FREE_FLOAT_PCT")
        request.getElement("fields").appendValue("PCT_FLT_SHARES_INSTITUTIONS")
        request.getElement("fields").appendValue("BETA_ADJ_OVERRIDABLE")
        request.getElement("fields").appendValue("VOLATILITY_90D")
        request.getElement("fields").appendValue("VOLATILITY_360D")
        request.getElement("fields").appendValue("PCT_INSIDER_SHARES_OUT")
        request.getElement("fields").appendValue("CHAIRMAN_AGE")
        request.getElement("fields").appendValue("CHAIRMAN_TENURE")
        request.getElement("fields").appendValue("PCT_WOMEN_MGT")
        request.getElement("fields").appendValue("TOT_SALARES_&_BNS_PD_TO_EXECS")
        request.getElement("fields").appendValue("CLASSIFIED_BOARD_SYSTEM")
        request.getElement("fields").appendValue("ARD_SHARES_ISSUED")
        # request.getElement("fields").appendValue("CHIEF_EXECUTIVE_OFFICER_TENURE")
        # request.getElement("fields").appendValue("CHIEF_EXECUTIVE_OFFICER_AGE")
        # request.getElement("fields").appendValue("CEO_FNDR_CO_FNDR_NOT_A_FNDR")
        # request.getElement("fields").appendValue("FEMALE_CEO_OR_EQUIVALENT")
        # request.getElement("fields").appendValue("CF_STOCK_BASED_COMPENSATION")
        # request.getElement("fields").appendValue("TOT_COMPENSATION_AW_TO_EXECS")
        # request.getElement("fields").appendValue("AVDR_STK_BASED_COMPENSATION_EXP")
        # request.getElement("fields").appendValue("AVG_BOD_TOTAL_COMPENSATION")
        # request.getElement("fields").appendValue("TOT_COMP_AW_TO_CEO_&_EQUIV")
        # request.getElement("fields").appendValue("AVG_EXECUTIVE_TOT_COMPENSATION")
        # request.getElement("fields").appendValue("TOT_BONUSES_PAID_TO_CEO_&_EQUIV")
        # request.getElement("fields").appendValue("TOT_COMP_AW_TO_CEO_&_EQUIV")
        # request.getElement("fields").appendValue("HIGHEST_BONUS_AMOUNT_PAID")
        # request.getElement("fields").appendValue("PCT_WOMEN_ON_BOARD")
        # request.getElement("fields").appendValue("BOARD_SIZE")
        # request.getElement("fields").appendValue("BOARD_AVERAGE_TENURE")
        # request.getElement("fields").appendValue("BOARD_AVERAGE_AGE")
        # request.getElement("fields").appendValue("PCT_INDEPENDENT_DIRECTORS")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "QUARTERLY")
        request.set("startDate", "19900331")
        request.set("endDate", "20190630")

        print("Sending Request:", request)
        # Send the request
        session.sendRequest(request)

        tickers = []
        dates = []
        EQY_FLOAT = []
        EQY_FREE_FLOAT_PCT = []
        PCT_FLT_SHARES_INSTITUTIONS = []
        BETA_ADJ_OVERRIDABLE = []
        VOLATILITY_360D = []
        VOLATILITY_90D = []
        PCT_INSIDER_SHARES_OUT = []
        CHAIRMAN_AGE = []
        CHAIRMAN_TENURE = []
        PCT_WOMEN_MGT = []
        TOT_SALARES_AND_BNS_PD_TO_EXECS = []
        CLASSIFIED_BOARD_SYSTEM = []
        ARD_SHARES_ISSUED = []

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
                        tickers.append(ticker)
                        dates.append(date.getElement('date').getValueAsString())
                        if(date.hasElement('EQY_FLOAT')):
                            EQY_FLOAT.append(date.getElement('EQY_FLOAT').getValueAsFloat())
                        else:
                            EQY_FLOAT.append(0)
                        if(date.hasElement('EQY_FREE_FLOAT_PCT')):
                            EQY_FREE_FLOAT_PCT.append(date.getElement('EQY_FREE_FLOAT_PCT').getValueAsFloat())
                        else:
                            EQY_FREE_FLOAT_PCT.append(0)
                        if(date.hasElement('PCT_FLT_SHARES_INSTITUTIONS')):
                            PCT_FLT_SHARES_INSTITUTIONS.append(date.getElement('PCT_FLT_SHARES_INSTITUTIONS').getValueAsFloat())
                        else:
                            PCT_FLT_SHARES_INSTITUTIONS.append(0)
                        if(date.hasElement('BETA_ADJ_OVERRIDABLE')):
                            BETA_ADJ_OVERRIDABLE.append(date.getElement('BETA_ADJ_OVERRIDABLE').getValueAsFloat())
                        else:
                            BETA_ADJ_OVERRIDABLE.append(0)
                        if(date.hasElement('VOLATILITY_90D')):
                            VOLATILITY_90D.append(date.getElement('VOLATILITY_90D').getValueAsFloat())
                        else:
                            VOLATILITY_90D.append(0)
                        if(date.hasElement('VOLATILITY_360D')):
                            VOLATILITY_360D.append(date.getElement('VOLATILITY_360D').getValueAsFloat())
                        else:
                            VOLATILITY_360D.append(0)
                        if(date.hasElement('PCT_INSIDER_SHARES_OUT')):
                            PCT_INSIDER_SHARES_OUT.append(date.getElement('PCT_INSIDER_SHARES_OUT').getValueAsFloat())
                        else:
                            PCT_INSIDER_SHARES_OUT.append(0)
                        if(date.hasElement('CHAIRMAN_AGE')):
                            CHAIRMAN_AGE.append(date.getElement('CHAIRMAN_AGE').getValueAsFloat())
                        else:
                            CHAIRMAN_AGE.append(0)
                        if(date.hasElement('CHAIRMAN_TENURE')):
                            CHAIRMAN_TENURE.append(date.getElement('CHAIRMAN_TENURE').getValueAsFloat())
                        else:
                            CHAIRMAN_TENURE.append(0)
                        if(date.hasElement('PCT_WOMEN_MGT')):
                            PCT_WOMEN_MGT.append(date.getElement('PCT_WOMEN_MGT').getValueAsFloat())
                        else:
                            PCT_WOMEN_MGT.append(0)
                        if(date.hasElement('TOT_SALARES_&_BNS_PD_TO_EXECS')):
                            TOT_SALARES_AND_BNS_PD_TO_EXECS.append(date.getElement('TOT_SALARES_&_BNS_PD_TO_EXECS').getValueAsFloat())
                        else:
                            TOT_SALARES_AND_BNS_PD_TO_EXECS.append(0)
                        if(date.hasElement('CLASSIFIED_BOARD_SYSTEM')):
                            CLASSIFIED_BOARD_SYSTEM.append(date.getElement('CLASSIFIED_BOARD_SYSTEM').getValueAsString())
                        else:
                            CLASSIFIED_BOARD_SYSTEM.append(0)
                        if(date.hasElement('ARD_SHARES_ISSUED')):
                            ARD_SHARES_ISSUED.append(date.getElement('ARD_SHARES_ISSUED').getValueAsFloat())
                        else:
                            ARD_SHARES_ISSUED.append(0)
                        

                if ev.eventType() == blpapi.Event.RESPONSE:
                    break

        for i in range(0, len(dates)):
            dates[i] = dates[i][0:4]+dates[i][5:7]+dates[i][8:10]
            if(dates[i][4:] == "1230"):
                dates[i] = dates[i][0:4] + "1231"
            if(dates[i][4:] == "0330"):
                dates[i] = dates[i][0:4] + "0331"
        finished = [tickers, dates, EQY_FLOAT,EQY_FREE_FLOAT_PCT,PCT_FLT_SHARES_INSTITUTIONS,BETA_ADJ_OVERRIDABLE,VOLATILITY_360D,VOLATILITY_90D,PCT_INSIDER_SHARES_OUT,CHAIRMAN_AGE,CHAIRMAN_TENURE,PCT_WOMEN_MGT,TOT_SALARES_AND_BNS_PD_TO_EXECS,CLASSIFIED_BOARD_SYSTEM,ARD_SHARES_ISSUED]
        import sqlite3
        finished = np.transpose(finished)
        connection = sqlite3.connect("bloombergMovement.db")
        crsr = connection.cursor()

        create_dict = """ CREATE TABLE if not exists bloombergMovement (
            ticker varchar,
            date varchar,
            EQY_FLOAT number,
            EQY_FREE_FLOAT_PCT number,
            PCT_FLOT_SHARES_INSTITUTIONS number,
            BETA_ADJ_OVERRIDABLE number,
            VOLATILITY_360D number,
            VOLATILITY_90D number,
            PCT_INSIDER_SHARES_OUT number,
            CHAIRMAN_AGE number,
            CHAIRMAN_TENURE number,
            PCT_WOMEN_MGT number,
            TOT_SALARES_AND_BNS_PD_TO_EXECS number,
            CLASSIFIED_BOARD_SYSTEM varchar,
            ARD_SHARES_ISSUED number
        ); """

        clear_dict = """DROP TABLE bloombergMovement;"""
        insertList = '''INSERT INTO bloombergMovement(ticker, date,EQY_FLOAT,EQY_FREE_FLOAT_PCT,PCT_FLOT_SHARES_INSTITUTIONS,BETA_ADJ_OVERRIDABLE,VOLATILITY_360D,VOLATILITY_90D,PCT_INSIDER_SHARES_OUT,CHAIRMAN_AGE,CHAIRMAN_TENURE,PCT_WOMEN_MGT,TOT_SALARES_AND_BNS_PD_TO_EXECS,CLASSIFIED_BOARD_SYSTEM,ARD_SHARES_ISSUED) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

        # crsr.execute(clear_dict)
        # print("table cleared")
        crsr.execute(create_dict)
        print("table created")
        for row in finished:
            crsr.execute(insertList,row)

        crsr.execute('''SELECT * from bloombergMovement''')
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