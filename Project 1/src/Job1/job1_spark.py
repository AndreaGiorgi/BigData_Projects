#!/usr/bin/env python3
"""spark application"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("job1_spark") \
    .getOrCreate()

def filter_rdd(line):

    if(line[0] !='ticker' and  line[7]!=None):
        try:
            datetime.strptime(line[7], "%Y-%m-%d")
            return True
        except ValueError:
            return False


files_RDD = spark.sparkContext.textFile(input_filepath)
lines_RDD =  files_RDD.map(f = lambda line: line.strip().split(',')).filter(filter_rdd)

tickers_RDD= lines_RDD.groupBy(lambda ticker: ticker[0])


def job(x):

    first_date_close_value = 0
    last_date_close_value = 0
    first_date = None
    last_date = None
    max_price = 0
    min_price = None
    
    for elem in x:
        try:
            date=datetime.strptime(elem[7], "%Y-%m-%d")
        except ValueError:
            continue
        if(first_date==None):

            first_date_close_value = float(elem[2])
            last_date_close_value = float(elem[2])
            first_date = date
            last_date = date
            min_price = float(elem[4])
            max_price = float(elem[5])
                
        else:
            if(date < first_date):
                first_date = date
                first_date_close_value = float(elem[2])
            else:
                if(date > last_date):
                    last_date = date
                    last_date_close_value = float(elem[2]) 
                
        if(min_price == None):
            min_price = float(elem[4])
        else:
            if(float(elem[4]) < min_price):
                 min_price = float(elem[4])
        if(float(elem[5]) > max_price):
            max_price = float(elem[5])
        
    
    variance = ((last_date_close_value - first_date_close_value )/first_date_close_value )*100
    return str(first_date), str(last_date), variance, max_price, min_price
    

close_value_ticker = tickers_RDD.mapValues(job)

get_result = close_value_ticker.sortBy(lambda x: x[1][1],ascending=False)

get_result.coalesce(1, shuffle=True).saveAsTextFile(output_filepath)