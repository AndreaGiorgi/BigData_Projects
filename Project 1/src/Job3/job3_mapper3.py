#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    ticker = ""
    name = ""
    month = ""
    variation = 0
        
    line = line.strip()
    columns = line.split('\t')

    if len(columns) == 3:
        ticker = str(columns[0])
        month = str(columns[1])
        variation = str(columns[2])
       
    if len(columns) == 2:
        ticker = str(columns[0])
        name = str(columns[1])

    print ("%s\t%s\t%s\t%s" % (ticker, name, month, variation))