#!/usr/bin/env python3
"""mapper.py"""

import sys

for record in sys.stdin:
    line = record.strip()
    elements = line.split("\t")
    if (len(elements) == 7):
        try:
            ticker, year, variation, volume, sector, first_date_cv, last_date_cv = elements
            if ticker and year and variation and volume and sector and first_date_cv and last_date_cv is not None:
                print('%s\t%s\t%s\t%s\t%s\t%s\t%s' %(ticker, year, variation, volume, sector, first_date_cv, last_date_cv))
        except ValueError:
            continue