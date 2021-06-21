#!/usr/bin/env python3
"""reducer.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split("\t")
    if (len(elements) == 2):
        try:
            ticker, name = elements
            if ticker and name is not None:
                print('%s,%s' %(str(ticker), str(name)))
        except ValueError:
            continue