#!/usr/bin/env python3
"""reducer.py"""

import sys

ticker_sector = {}
ticker_data = []


for line in sys.stdin:

    line = line.strip()
    ticker, sector, year, variation, volume, first_date_cv, last_date_cv = line.split("\t")

    #Se i due valori sono 0 allora i dati provengono da ticker-sector.txt
    if float(volume) == 0 and float(variation) == 0:
        ticker_sector[ticker] = sector
    #altrimenti da ticker-data.txt
    else:
        ticker_data.append([ticker, variation, volume, year, first_date_cv, last_date_cv])


for record in ticker_data:
    ticker = str(record[0])
    variation = str(record[1])
    volume = str(record[2])
    year = str(record[3])
    first_date_cv = str(record[4])
    last_date_cv = str(record[5])

    if ticker in ticker_sector:
        sector = str(ticker_sector[record[0]])
        if(sector != 'N/A'):
            print('%s\t%s\t%s\t%s\t%s\t%s\t%s' %(ticker, year, variation, volume, sector, first_date_cv, last_date_cv))

        else:
            continue


    


    
