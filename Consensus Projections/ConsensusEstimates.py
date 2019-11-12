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
        tickers = tickers[5000:6000]
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
        request.getElement("fields").appendValue("BEST_ESTIMATE_FCF")
        request.getElement("fields").appendValue("BEST_SALES")
        request.getElement("fields").appendValue("BEST_EBITDA")
        request.getElement("fields").appendValue("BEST_EBITDA_3MO_PCT_CHG")
        request.getElement("fields").appendValue("BEST_EBIT")
        request.getElement("fields").appendValue("BEST_EBIT_STDDEV")
        request.getElement("fields").appendValue("BEST_NET_INCOME")
        request.getElement("fields").appendValue("BEST_EPS")
        request.getElement("fields").appendValue("IS_COMP_GROSS_MARGIN_PERCENTAGE")
        request.getElement("fields").appendValue("BEST_EPS_3MO_PCT_CHG")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "QUARTERLY")
        request.set("startDate", "19900331")
        request.set("endDate", "20190630")

        print("Sending Request:", request)
        # Send the request
        session.sendRequest(request)

        tickers = []
        dates = []
        cons_revenue = []
        cons_profit_margin = []
        cons_ebitda = []
        cons_ebitda_qoq_change = []
        cons_ebit = []
        cons_ebitda_stddev = []
        cons_net_income = []
        cons_eps = []
        cons_eps_qoq_change = []
        cons_fcf = []

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
                        if(date.hasElement('BEST_SALES')):
                            cons_revenue.append(date.getElement('BEST_SALES').getValueAsFloat())
                        else:
                            cons_revenue.append(0)
                        if(date.hasElement('IS_COMP_GROSS_MARGIN_PERCENTAGE')):
                            cons_profit_margin.append(date.getElement('IS_COMP_GROSS_MARGIN_PERCENTAGE').getValueAsFloat())
                        else:
                            cons_profit_margin.append(0)
                        if(date.hasElement('BEST_EBITDA')):
                            cons_ebitda.append(date.getElement('BEST_EBITDA').getValueAsString())
                        else:
                            cons_ebitda.append(0)                                                    
                        if(date.hasElement('BEST_EBITDA_3MO_PCT_CHG')):
                            cons_ebitda_qoq_change.append(date.getElement('BEST_EBITDA_3MO_PCT_CHG').getValueAsString())
                        else:
                            cons_ebitda_qoq_change.append(0)
                        if(date.hasElement('BEST_EBIT')):
                            cons_ebit.append(date.getElement('BEST_EBIT').getValueAsFloat())
                        else:
                            cons_ebit.append(0)
                        if(date.hasElement('BEST_EBIT_STDDEV')):
                            cons_ebitda_stddev.append(date.getElement('BEST_EBIT_STDDEV').getValueAsFloat())
                        else:
                            cons_ebitda_stddev.append(0)
                        if(date.hasElement('BEST_NET_INCOME')):
                            cons_net_income.append(date.getElement('BEST_NET_INCOME').getValueAsFloat())
                        else:
                            cons_net_income.append(0)
                        if(date.hasElement('BEST_EPS')):
                            cons_eps.append(date.getElement('BEST_EPS').getValueAsFloat())
                        else:
                            cons_eps.append(0)
                        if(date.hasElement('BEST_EPS_3MO_PCT_CHG')):
                            cons_eps_qoq_change.append(date.getElement('BEST_EPS_3MO_PCT_CHG').getValueAsFloat())
                        else:
                            cons_eps_qoq_change.append(0)
                        if(date.hasElement('BEST_ESTIMATE_FCF')):
                            cons_fcf.append(date.getElement('BEST_ESTIMATE_FCF').getValueAsFloat())
                        else:
                            cons_fcf.append(0)
                
                                                                                                                                                                                                                                                                                                                                                                                                                                    
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break

        for i in range(0, len(dates)):
            dates[i] = dates[i][0:4]+dates[i][5:7]+dates[i][8:10]
            if(dates[i][4:] == "1230"):
                dates[i] = dates[i][0:4] + "1231"
            if(dates[i][4:] == "0330"):
                dates[i] = dates[i][0:4] + "0331"
        finished = [tickers, dates,cons_revenue,cons_profit_margin,cons_ebitda,cons_ebitda_qoq_change,cons_ebit,cons_ebitda_stddev,cons_net_income,cons_eps,cons_eps_qoq_change,cons_fcf]
        import sqlite3
        finished = np.transpose(finished)
        print(finished)
        connection = sqlite3.connect("analystConsensus.db")
        crsr = connection.cursor()

        create_dict = """ CREATE TABLE if not exists consensus (
            ticker varchar,
            date varchar,
            cons_revenue number,
            cons_profit_margin number,
            cons_ebitda number,
            cons_ebitda_qoq_change number,
            cons_ebit number,
            cons_ebitda_stddev number,
            cons_net_income number,
            cons_eps number,
            cons_eps_qoq_change number,
            cons_fcf number
        ); """

        clear_dict = """DROP TABLE consensus;"""
        insertList = '''INSERT INTO consensus(ticker, date,cons_revenue,cons_profit_margin,cons_ebitda,cons_ebitda_qoq_change,cons_ebit,cons_ebitda_stddev,cons_net_income,cons_eps,cons_eps_qoq_change,cons_fcf) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'''

        # crsr.execute(clear_dict)
        # print("table cleared")
        crsr.execute(create_dict)
        print("table created")
        for row in finished:
            print(row)
            crsr.execute(insertList,row)

        crsr.execute('''SELECT * from consensus''')
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