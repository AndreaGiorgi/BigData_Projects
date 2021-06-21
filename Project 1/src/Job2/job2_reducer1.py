#!/usr/bin/env python3
"""reducer.py"""

import collections
import sys
from datetime import datetime

## Reducer secondo punto progetto:
#
# 1. Variazione percentuale ciascun settore  per ogni anno
# 2. Per ciascun settore quale è l'azione con la variazione percentuale maggiore
# 3. Per ciascun settore quale è l'azione con volume maggiore
#
# I risultati devi essere ordinati sulla base del settore

historical_stocks_prices = {}

for record in sys.stdin:
    line=record.strip()
    ticker, open_value, close_value, low, high, volume, date = line.split("\t")
    
    try:
        current_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        continue
    
    if ticker not in historical_stocks_prices:
        historical_stocks_prices[ticker] = {}
        historical_stocks_prices[ticker][current_date.year] = {}
        historical_stocks_prices[ticker][current_date.year]["first_date"] = current_date
        historical_stocks_prices[ticker][current_date.year]["first_date_close_value"] = float(close_value)
        historical_stocks_prices[ticker][current_date.year]["last_date"] = current_date
        historical_stocks_prices[ticker][current_date.year]["last_date_close_value"] =float( close_value)
        historical_stocks_prices[ticker][current_date.year]["volume"] = float(volume)
        continue
    if current_date.year not in historical_stocks_prices[ticker]:
        historical_stocks_prices[ticker][current_date.year] = {}
        historical_stocks_prices[ticker][current_date.year]["first_date"] = current_date
        historical_stocks_prices[ticker][current_date.year]["first_date_close_value"] = float(close_value)
        historical_stocks_prices[ticker][current_date.year]["last_date"] = current_date
        historical_stocks_prices[ticker][current_date.year]["last_date_close_value"] = float(close_value)
        historical_stocks_prices[ticker][current_date.year]["volume"] = float(volume)
        continue
    #prima e ultima data e rispettivi valori di chiusura
    if current_date < historical_stocks_prices[ticker][current_date.year]["first_date"]:
            historical_stocks_prices[ticker][current_date.year]["first_date"] = current_date
            historical_stocks_prices[ticker][current_date.year]["first_date_close_value"] = float(close_value)
    else:
        if current_date > historical_stocks_prices[ticker][current_date.year]["last_date"]:
            historical_stocks_prices[ticker][current_date.year]["last_date"] = current_date
            historical_stocks_prices[ticker][current_date.year]["last_date_close_value"] = float(close_value)

    #volume annuale
    historical_stocks_prices[ticker][current_date.year]["volume"] += float(volume)
    
for ticker in historical_stocks_prices:
    for year in historical_stocks_prices[ticker]:
        app = historical_stocks_prices[ticker][year]["last_date_close_value"]-historical_stocks_prices[ticker][year]["first_date_close_value"]
        variation = (app/historical_stocks_prices[ticker][year]["first_date_close_value"])*100
        print('%s\t%s\t%s\t%s\t%s\t%s' %(str(ticker), str(year), str(variation), str(historical_stocks_prices[ticker][year]["volume"]), str(historical_stocks_prices[ticker][year]["first_date_close_value"]), str(historical_stocks_prices[ticker][year]["last_date_close_value"])))
        