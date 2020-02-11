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

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        for ticker in tickers:
            request.getElement("securities").appendValue(ticker[0] + " US Equity")
        request.getElement("fields").appendValue("EQY_INST_HOLD")
        request.getElement("fields").appendValue("EQY_INST_BUYS")
        request.getElement("fields").appendValue("EQY_INST_SELLS")
        request.getElement("fields").appendValue("EQY_INST_PCT_SH_OUT")
        request.getElement("fields").appendValue("CHG_PCT_5D")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "WEEKLY")
        request.set("startDate", "20110101")
        request.set("endDate", "20181231")
        request.set("maxDataPoints", 100)

        print("Sending Request:", request)
        # Send the request
        session.sendRequest(request)

        buys = []
        sells = []
        px_change = []
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in ev:
                    comp_buys = []
                    comp_sells = []
                    comp_px_change = []
                    sd = msg.getElement('securityData').getElement('fieldData').values()
                    for date in sd:
                        print(date)
                        if(date.hasElement('EQY_INST_BUYS') and date.hasElement('EQY_INST_SELLS') and date.hasElement('CHG_PCT_5D')):
                            comp_buys.append(date.getElement('EQY_INST_BUYS').getValueAsFloat())
                            comp_sells.append(date.getElement('EQY_INST_SELLS').getValueAsFloat())
                            comp_px_change.append(date.getElement('CHG_PCT_5D').getValueAsFloat())
                        else:
                            print("missing field")
                    buys.append(comp_buys[:len(comp_buys)-1])
                    sells.append(comp_sells[:len(comp_sells)-1])
                    px_change.append(comp_px_change[1:])

                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        inst_change = []
        px_change_flat = []
        for i in range(0, len(buys)):
            for j in range(0, len(buys[i])):
                inst_change.append(buys[i][j]/(buys[i][j]+sells[i][j]))
                px_change_flat.append(px_change[i][j])
        print(inst_change)
        print(len(px_change_flat))
        print(np.corrcoef(inst_change, px_change_flat))
        plt.scatter(inst_change, px_change_flat, alpha=0.5)
        plt.xlabel('Institutional Buying Index')
        plt.ylabel('% Change in Share Price (following week)')
        plt.show()


    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print("SimpleHistoryExample")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")


