#!/usr/bin/env python3
"""reducer.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split("\t")
    if (len(elements) == 2):
        try:
            ticker, sector = elements
            if ticker and sector is not None:
                print('%s,%s' %(str(ticker), str(sector)))
        except ValueError:
            continue