#!/usr/bin/env python3
"""reducer.py"""

import collections
import operator
import sys
from datetime import datetime

## Reducer primo punto progetto:
#
# Per ciascuna azione bisogna ricavare
# 1. Data prima quotazione
# 2. Data ultima quotazione  
# 3. Variazione percentuale quotazione, ossia differenza percentuale prima e ultima quotazione
# 4. Prezzo massimo e prezzo minimo
# 5. Giorni consecutivi in cui la quotazione è cresciuta
#
# I risultati devi essere ordinati sulla base del punto 2

historical_stocks_prices = {}

for record in sys.stdin:
    
    line = record.strip()
    ticker, open_value, close_value, low, high, date = line.split("\t")

    ## Necessario per il corretto funzionamento
    open_value = float(open_value)
    close_value = float(close_value)
    low = float(low)
    high = float(high)
    
    try:
        stock_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        continue
    
    if ticker in historical_stocks_prices:
        
        ## Punto a: Calcolo della data prima quotazione   
        if stock_date < historical_stocks_prices[ticker]["first_quotation_date"]:
            historical_stocks_prices[ticker]["first_quotation_date"] = stock_date
            historical_stocks_prices[ticker]["ov_firstdate"] = open_value
            historical_stocks_prices[ticker]["cv_lastdate"] = close_value
        
        ##Punto b: Calcolo della data ultima quotazione
        if stock_date > historical_stocks_prices[ticker]["last_quotation_date"]:
            historical_stocks_prices[ticker]["last_quotation_date"] = stock_date
            historical_stocks_prices[ticker]["ov_lastdate"] = open_value
            historical_stocks_prices[ticker]["cv_lastdate"] = close_value
        
        ##Punto c: Calcolo della variazione percentuale della quotazione
        historical_stocks_prices[ticker]["variance"] = ((historical_stocks_prices[ticker]["cv_lastdate"] - historical_stocks_prices[ticker]["cv_firstdate"] / 
                                                        historical_stocks_prices[ticker]["cv_firstdate"]) * 100)
            
        ##Punto d: Calcolo del prezzo massimo e del prezzo minimo
        if low < historical_stocks_prices[ticker]["min_price"]:
            historical_stocks_prices[ticker]["min_price"] = low
        if high > historical_stocks_prices[ticker]["max_price"]:
            historical_stocks_prices[ticker]["max_price"] = high

        ##Punto e: Calcolo giorni consecutivi di crescita valore di chiusura
        if close_value > open_value:
            if historical_stocks_prices[ticker]["temp_year_max_increase"] == stock_date.year:
                    historical_stocks_prices[ticker]["temp_count"] += 1
            else:
                if historical_stocks_prices[ticker]["temp_count"] > historical_stocks_prices[ticker]["consecutive_cv_increment_days"]:
                    historical_stocks_prices[ticker]["consecutive_cv_increment_days"] = historical_stocks_prices[ticker]["temp_count"]
                    historical_stocks_prices[ticker]["year_max_increase"] = historical_stocks_prices[ticker]["temp_year_max_increase"]
                historical_stocks_prices[ticker]["temp_count"] = 0
                historical_stocks_prices[ticker]["temp_year_max_increase"] = stock_date.year
        else:
            if historical_stocks_prices[ticker]["temp_count"] > historical_stocks_prices[ticker]["consecutive_cv_increment_days"]:                
                historical_stocks_prices[ticker]["consecutive_cv_increment_days"] = historical_stocks_prices[ticker]["temp_count"]
                historical_stocks_prices[ticker]["year_max_increase"] = historical_stocks_prices[ticker]["temp_year_max_increase"]
            historical_stocks_prices[ticker]["temp_count"] = 0
            historical_stocks_prices[ticker]["temp_year_max_increase"] = stock_date.year 
    else:

        historical_stocks_prices[ticker] = {}
        historical_stocks_prices[ticker]["ticker"] = ticker
        historical_stocks_prices[ticker]["first_quotation_date"] = stock_date
        historical_stocks_prices[ticker]["last_quotation_date"] = stock_date
        historical_stocks_prices[ticker]["variance"] = 0
        historical_stocks_prices[ticker]["max_price"] = float(high)
        historical_stocks_prices[ticker]["min_price"] = float(low)
        historical_stocks_prices[ticker]["ov_firstdate"] = float(open_value)
        historical_stocks_prices[ticker]["cv_firstdate"] = float(close_value)
        historical_stocks_prices[ticker]["ov_lastdate"] = float(open_value)
        historical_stocks_prices[ticker]["cv_lastdate"] = float(close_value)
        historical_stocks_prices[ticker]["consecutive_cv_increment_days"] = 0
        historical_stocks_prices[ticker]["temp_count"] = 0
        historical_stocks_prices[ticker]["year_max_increase"] = stock_date.year
        historical_stocks_prices[ticker]["temp_year_max_increase"] = stock_date.year

## La stampa dei risultati deve essere ordinata secondo il punto 2
results = collections.OrderedDict(sorted(historical_stocks_prices.items(), key = lambda x: operator.getitem(x[1], "last_quotation_date"), reverse=True))

##Inutile tanto non si vede nel file a lungo termine, utile in fase di debug
print("Ticker\tFirst_quotation_date\tLast_quotation_date\tVariance\tMin_price\tMax_price\tIncreasing_CV_Consecutive_Days\tYear_Max_Increase")

##Invio in stdout dei risultati
for result in results:
    
    consecutive_days = 0
    if results[result]['temp_count'] > results[result]['consecutive_cv_increment_days']:
        consecutive_days = results[result]['consecutive_cv_increment_days']
        year_max_increase = results[result]['temp_year_max_increase']
    else: 
        consecutive_days = results[result]['temp_count']
        year_max_increase = results[result]['year_max_increase']
        
    ticker = results[result]["ticker"]
    first_quotation_date = results[result]["first_quotation_date"]
    last_quotation_date = results[result]["last_quotation_date"]
    variance= results[result]["variance"]
    low= results[result]["min_price"]
    high= results[result]["max_price"]
    consecutive_days = results[result]["consecutive_cv_increment_days"]
    year_max_increase = results[result]["year_max_increase"]
    
    print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %(str(ticker),str(first_quotation_date),str(last_quotation_date),str(variance), str(low),str(high),str(consecutive_days), str(year_max_increase)))