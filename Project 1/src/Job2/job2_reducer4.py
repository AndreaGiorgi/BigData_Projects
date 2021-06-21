#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

sector_year = {}

for line in sys.stdin:

    line = line.strip()
    ticker, year, variation, volume, sector, first_date_cv, last_date_cv = line.split("\t")

    if sector not in sector_year:
        sector_year[sector] = {}
        sector_year[sector][year] = {}
        sector_year[sector][year]['first_date_value_sum'] = float(first_date_cv)
        sector_year[sector][year]['last_date_value_sum'] = float(last_date_cv)
        sector_year[sector][year]['ticker_variation'] = [ticker, float(variation)]
        sector_year[sector][year]['ticker_volume'] = [ticker, float(volume)]
        continue 

    if sector in sector_year:
        if year not in sector_year[sector]:
            sector_year[sector][year] = {}
            sector_year[sector][year]['first_date_value_sum'] = float(first_date_cv)
            sector_year[sector][year]['last_date_value_sum'] = float(last_date_cv)
            sector_year[sector][year]['ticker_variation'] = [ticker, float(variation)]
            sector_year[sector][year]['ticker_volume'] = [ticker, float(volume)]

        else: 
            sector_year[sector][year]['first_date_value_sum'] += float(first_date_cv)
            sector_year[sector][year]['last_date_value_sum'] += float(last_date_cv)

            if float(variation) > sector_year[sector][year]['ticker_variation'][1]:
                sector_year[sector][year]['ticker_variation'] = [ticker,float(variation)]
            if float(volume) > sector_year[sector][year]['ticker_volume'][1]:
                sector_year[sector][year]['ticker_volume'] = [ticker,float(volume)]

results = collections.OrderedDict(sorted(sector_year.items(),reverse=False))


for result in results:
    for year in sector_year[result]:  
        total_variation = ((sector_year[result][year]['last_date_value_sum'] - sector_year[result][year]['first_date_value_sum']) / sector_year[result][year]['first_date_value_sum'])*100
        ticker_variation = sector_year[result][year]['ticker_variation']
        ticker_volume = sector_year[result][year]['ticker_volume']

        print("%s\t%s\t%s\t%s\t%s\t%s\t%s" % (str(result), str(year), str(total_variation), str(ticker_variation[0]), str(ticker_variation[1]), str(ticker_volume[0]), str(ticker_volume[1])))

