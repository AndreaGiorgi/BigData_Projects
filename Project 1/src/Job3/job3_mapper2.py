#!/usr/bin/env python3
"""mapper.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split(",")
    if (len(elements) == 5):
        try:
            ticker, exchange, name, sector, industry = elements
            if ticker and exchange and name and sector and industry is not None:
                print('%s\t%s' %(str(ticker), str(name)))
        except ValueError:
            continue