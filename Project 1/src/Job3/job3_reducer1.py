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
    line = record.strip()
    ticker, close_value, date = line.split("\t")
    
    try:
        current_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        continue
    
    if ticker not in historical_stocks_prices:
        historical_stocks_prices[ticker] = {}
        historical_stocks_prices[ticker][current_date.month] = {}
        historical_stocks_prices[ticker][current_date.month]["first_date"] = current_date
        historical_stocks_prices[ticker][current_date.month]["first_date_close_value"] = float(close_value)
        historical_stocks_prices[ticker][current_date.month]["last_date"] = current_date
        historical_stocks_prices[ticker][current_date.month]["last_date_close_value"] = float(close_value)
        continue
    
    if current_date.month not in historical_stocks_prices[ticker]:
        historical_stocks_prices[ticker][current_date.month] = {}
        historical_stocks_prices[ticker][current_date.month]["first_date"] = current_date
        historical_stocks_prices[ticker][current_date.month]["first_date_close_value"] = float(close_value)
        historical_stocks_prices[ticker][current_date.month]["last_date"] = current_date
        historical_stocks_prices[ticker][current_date.month]["last_date_close_value"] = float(close_value)
        continue

    #prima e ultima data e rispettivi valori di chiusura
    if current_date < historical_stocks_prices[ticker][current_date.month]["first_date"]:
            historical_stocks_prices[ticker][current_date.month]["first_date"] = current_date
            historical_stocks_prices[ticker][current_date.month]["first_date_close_value"] = float(close_value)
    else:
        if current_date > historical_stocks_prices[ticker][current_date.month]["last_date"]:
            historical_stocks_prices[ticker][current_date.month]["last_date"] = current_date
            historical_stocks_prices[ticker][current_date.month]["last_date_close_value"] = float(close_value)

    
for ticker in historical_stocks_prices:
    for month in historical_stocks_prices[ticker]:
        app = historical_stocks_prices[ticker][month]["last_date_close_value"]-historical_stocks_prices[ticker][month]["first_date_close_value"]
        variation = (app/historical_stocks_prices[ticker][month]["first_date_close_value"])*100
        print('%s\t%s\t%s' %(str(ticker), str(month), str(variation)))
        