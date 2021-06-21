#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

name_month = {}

months={}
months['1']="GEN"
months['2']="FEB"
months['3']="MAR"
months['4']="APR"
months['5']="MAG"
months['6']="GIU"
months['7']="LUG"
months['8']="AGO"
months['9']="SET"
months['10']="OTT"
months['11']="NOV"
months['12']="DIC"

def check(ticker, ticker1,name_ticker,name_ticker1):
    
    temp=[]
    s = 1

    for month_a in ticker:
        for month_b in ticker1:
            if(month_a == month_b):
                difference = abs(float(ticker[month_a]['variation']) - float(ticker1[month_a]['variation']))
                if(difference>=0 and difference <= s):
                    pairs_names = "{"+name_ticker+","+name_ticker1+"}"
                    if pairs_names not in temp:
                         temp.append(pairs_names)
                         temp.append(months[month_a]+":"+name_ticker+" "+str(ticker[month_a]['variation']))
                         temp.append(months[month_a]+":"+name_ticker1+" "+str(ticker1[month_a]['variation']))
                    else:
                        temp.append(months[month_a]+":"+name_ticker+" "+str(ticker[month_a]['variation']))
                        temp.append(months[month_a]+":"+name_ticker1+" "+str(ticker1[month_a]['variation']))

    for elem in temp:
        print("%s\t%s\t" % (str(s), elem))


for line in sys.stdin:

    line = line.strip()
    name, month, variation = line.split("\t")
    
    if name not in name_month:
        name_month[name] = {}
        name_month[name][month] = {}
        name_month[name][month]['variation'] = float(variation)
        continue 

    if name in name_month:
        if month not in name_month[name]:
            name_month[name][month] = {}
            name_month[name][month]['variation'] = float(variation)

results = collections.OrderedDict(sorted(name_month.items(),reverse=False))

names = list(name_month.keys())

for i in range(0, len(names)):
    name_ticker=names[i]
    ticker = name_month[names[i]]
    j = i+1

    for n in range(j, len(names)):
        name_ticker1=names[n]
        ticker1 = name_month[names[n]]
        check(ticker, ticker1,name_ticker,name_ticker1)      