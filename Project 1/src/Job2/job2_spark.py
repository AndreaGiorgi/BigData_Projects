#!/usr/bin/env python3
"""spark application"""

from datetime import datetime
import argparse

from datetime import datetime
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--input2_path", type=str, help="Input 2 file path")
parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath, input2_filepath, output_filepath  = args.input_path, args.input2_path, args.output_path

spark = SparkSession.builder.appName("job2_spark").getOrCreate()

ticker_data = spark.sparkContext.textFile(input_filepath)
ticker_sector = spark.sparkContext.textFile(input2_filepath)

def ticker_year(row):
    ticker = row[0]
    year = datetime.strptime(row[7], "%Y-%m-%d").year
    return (ticker,year)

def filter_by_year(row):
    key = row[0]
    return key[1] > 2008 and key[1] < 2019

records_rdd = ticker_data.map(lambda row: row.split(",")).filter(lambda row: row[0] != 'ticker').groupBy(ticker_year).filter(filter_by_year)
ticker_sector = ticker_sector.map(lambda row: row.split(",")).groupBy(lambda row: row[0])

def f1(row):
    l = []
    for elem in row:
        l.append(elem)
    return l

grouped_by_ticker_year = records_rdd.mapValues(f1)

def f2(row):
    _obj = row[1]

    min_date = None
    max_date = None
    first_date_close_value = 0
    last_date_close_value = 0
    volume = 0

    for record in _obj:
        ticker = record[0]
        close_value = float(record[2])
        volume += int(record[6])
        date = datetime.strptime(record[7],"%Y-%m-%d")
        
        if min_date == None:
            min_date = datetime.strptime(record[7],"%Y-%m-%d")
            first_date_close_value = close_value
        else: 
            if date < min_date:
                min_date = date
                first_date_close_value = close_value
        if max_date == None:
            max_date = datetime.strptime(record[7],"%Y-%m-%d")
            last_date_close_value = close_value
        else:
            if(date > max_date):
                max_date = date
                last_date_close_value = close_value
    
    variation = (((last_date_close_value - first_date_close_value)/first_date_close_value)*100)

    return ticker, min_date.year, variation, volume, first_date_close_value, last_date_close_value

grouped_by_ticker_year = grouped_by_ticker_year.map(f2)

g_ticker = grouped_by_ticker_year.map(lambda x: (x[0],(x[1],x[2],x[3],x[4],x[5])))
s_ticker = ticker_sector.map(lambda x: (x[0],x[1]))

def f3(row):
    _values = row[0]
    _obj = row[1]

    for elem in _obj:
        return _values[0],_values[1], _values[2],_values[3], _values[4],elem[1]

gs_join = g_ticker.join(s_ticker).mapValues(f3)

def f4(row):
    return (row[1][5],row[1][0]),(row[0],float(row[1][1]),int(row[1][2]))


max_variation = gs_join.map(f4).reduceByKey(lambda x,y: x if x[1] > y[1] else y)
max_volume = gs_join.map(f4).reduceByKey(lambda x,y: x if x[2] > y[2] else y)

sum_fclose = gs_join.map(lambda row: ((row[1][5],row[1][0]),(float(row[1][4])))).reduceByKey(lambda x,y: x+y)
sum_lclose = gs_join.map(lambda row: ((row[1][5],row[1][0]),(float(row[1][3])))).reduceByKey(lambda x,y: x+y)

##(row[1][1]-row[1][0])/row[1][0])*100)
variation = sum_lclose.join(sum_fclose).map(lambda row: ((row[0]), (row[1][1]-row[1][0])/row[1][0])*100)

def f5(row):
    sector = row[0][0]
    year = row[0][1]
    _obj = row[1]
    variation = _obj[1]
    _info = _obj[0]
    max_variation = (_info[0][0],_info[0][1])
    max_volume = (_info[1][0],_info[1][2])
    
    return (sector, year, variation, max_variation, max_volume)

def fprint(row):
    return "%s\t%s\t%s\t%s\t%s" % (row[0],row[1],row[2],row[3],row[4])

output = max_variation.join(max_volume).join(variation).map(f5).sortBy(lambda row: (row[0],row[1])).map(fprint)

output.saveAsTextFile(output_filepath)