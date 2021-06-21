#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime

for record in sys.stdin:
    line = record.strip()
    elements = line.split(",")
    if (len(elements) == 8):
        try:
            ticker, open_value, close_value, _, low, high, volume, date = elements
            if ticker and open_value and close_value and low and high and volume and date is not None:
                date1 = datetime.strptime(date, "%Y-%m-&d")
                if date1.year > 2008 and date1.year < 2019:
                    print('%s\t%f\t%f\t%f\t%f\t%i\t%s' %(str(ticker), float(open_value), float(close_value), float(low), float(high), int(volume), str(date)))
        except ValueError:
            continue