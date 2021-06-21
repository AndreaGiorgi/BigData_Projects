#!/usr/bin/env python3
"""mapper.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split(",")
    if (len(elements) == 8):
        try:
            ticker, open_value, close_value, _, low, high, __, date = elements
            if ticker and open_value and close_value and low and high and date is not None:
                print('%s\t%f\t%f\t%f\t%f\t%s' %(str(ticker), float(open_value), float(close_value), float(low), float(high), str(date)))
        except ValueError:
            continue