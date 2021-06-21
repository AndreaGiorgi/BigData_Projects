#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    ticker = ""
    sector = ""
    year = ""
    variation = 0
    volume = 0
    first_date_close_value=0
    last_date_close_value=0
    line = line.strip()
    columns = line.split('\t')

    if len(columns) == 6:
        ticker = str(columns[0])
        year = str(columns[1])
        variation = str(columns[2])
        volume = str(columns[3])
        first_date_close_value = str(columns[4])
        last_date_close_value = str(columns[5])
       
    else:
        if len(columns) == 2:
            ticker = str(columns[0])
            sector = str(columns[1])

    print ( "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (ticker, sector, year, variation, volume, first_date_close_value, last_date_close_value) )
    



