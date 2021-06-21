#!/usr/bin/env python3
"""mapper.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split("\t")
    if (len(elements) == 4):
        try:
            ticker, name, month, variation = elements
            if ticker and name and month and variation is not None:
                print('%s\t%s\t%s' %(name, month, variation))
        except ValueError:
            continue