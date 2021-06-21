#!/usr/bin/env python3
"""reducer.py"""

import sys

ticker_name = {}
ticker_data = []


for line in sys.stdin:

    line = line.strip()
    ticker, name, month, variation = line.split("\t")

    #Se i due valori sono 0 allora i dati provengono da ticker-name.txt
    if float(variation) == 0 and month == "":
        ticker_name[ticker] = name
    #altrimenti da ticker-data.txt
    else:
        ticker_data.append([ticker, name, month, variation])


for record in ticker_data:
    ticker = str(record[0])
    name = str(record[1])
    month = str(record[2])
    variation = str(record[3])

    if ticker in ticker_name:
        name = str(ticker_name[record[0]])
        print('%s\t%s\t%s\t%s' %(ticker, name, month, variation))
    else:
        continue