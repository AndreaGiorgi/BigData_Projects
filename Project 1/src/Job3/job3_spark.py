#!/usr/bin/env python3
"""spark application"""

from datetime import datetime
import argparse

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")
parser.add_argument("--input_path1", type=str, help="Input file path1")

args = parser.parse_args()

input_filepath, output_filepath  = args.input_path, args.output_path
input_filepath1= args.input_path1

spark = SparkSession.builder.appName("job3_spark").getOrCreate()

ticker_data = spark.sparkContext.textFile(input_filepath)
ticker_name_month = spark.sparkContext.textFile(input_filepath1)


months={}
months['1']="GEN"
months['2']="FEB"
months['3']="MAR"
months['4']="APR"
months['5']="MAG"
months['6']="GIU"
months['7']="LUG"
months['8']="AGO"
months['9']="SET"
months['10']="OTT"
months['11']="NOV"
months['12']="DIC"

def ticker_year(line):
    year = datetime.strptime(line[7], "%Y-%m-%d").year
    return year == 2017


records_rdd = ticker_data.map(lambda line: line.split(",")).filter(lambda line: line[0] != 'ticker').filter(ticker_year).groupBy(lambda line:line[0])
ticker_name_month = ticker_name_month.map(lambda line: line.split(",")).map(lambda line:(line[0],line[2]))

def f(x):
    ticker_mounth={}
    for elem in x:
        try:
            current_date=datetime.strptime(elem[7], "%Y-%m-%d")
        except ValueError:
            continue
        if elem[0] not in ticker_mounth:
            ticker_mounth[elem[0]]={}
            ticker_mounth[elem[0]][current_date.month]={}
            ticker_mounth[elem[0]][current_date.month]['first_date'] = current_date
            ticker_mounth[elem[0]][current_date.month]['last_date'] = current_date
            ticker_mounth[elem[0]][current_date.month]['first_date_close_value'] = float(elem[2])
            ticker_mounth[elem[0]][current_date.month]['last_date_close_value'] = float(elem[2])
            ticker_mounth[elem[0]][current_date.month]['variation'] = 0
            continue

        if current_date.month not in ticker_mounth[elem[0]]:
            ticker_mounth[elem[0]][current_date.month]={}
            ticker_mounth[elem[0]][current_date.month]['first_date'] = current_date
            ticker_mounth[elem[0]][current_date.month]['last_date'] = current_date
            ticker_mounth[elem[0]][current_date.month]['first_date_close_value'] = float(elem[2])
            ticker_mounth[elem[0]][current_date.month]['last_date_close_value'] = float(elem[2])
            ticker_mounth[elem[0]][current_date.month]['variation'] = 0
            continue

        if(current_date< ticker_mounth[elem[0]][current_date.month]['first_date']):
            ticker_mounth[elem[0]][current_date.month]['first_date'] = current_date
            ticker_mounth[elem[0]][current_date.month]['first_date_close_value'] = float(elem[2])
            ticker_mounth[elem[0]][current_date.month]['variation'] = ((ticker_mounth[elem[0]][current_date.month]['last_date_close_value'] - ticker_mounth[elem[0]][current_date.month]['first_date_close_value'])/ticker_mounth[elem[0]][current_date.month]['first_date_close_value'])*100
        else:
            if(current_date> ticker_mounth[elem[0]][current_date.month]['last_date']):
                ticker_mounth[elem[0]][current_date.month]['last_date']=current_date
                ticker_mounth[elem[0]][current_date.month]['last_date_close_value']=float(elem[2])
                ticker_mounth[elem[0]][current_date.month]['variation']=((ticker_mounth[elem[0]][current_date.month]['last_date_close_value'] - ticker_mounth[elem[0]][current_date.month]['first_date_close_value'])/ticker_mounth[elem[0]][current_date.month]['first_date_close_value'])*100
    
    ticker=[]
    try:
        for elem in ticker_mounth:
            for month in ticker_mounth[elem]:
                ticker.append([elem,ticker_mounth[elem][month]['last_date'].month,ticker_mounth[elem][month]['variation']])
    
        return ticker
    except KeyError:
        return []

def compare(ticker,ticker1,ticker_month,ticker1_month):
    app=[]
    for month in ticker_month:
        for month_1 in ticker1_month:
            if(month[1]==month_1[1]):
                difference=abs(float(month[2])-float(month_1[2]))
                if(difference >= 0 and difference <= 1):
                    pairs_ticker="{"+ticker+","+ticker1+"}"
                    if pairs_ticker not in app:
                        app.append(pairs_ticker)
                        app.append(months[str(month[1])]+":"+month[0]+" "+str(month[2]))
                        app.append(months[str(month[1])]+":"+month_1[0]+" "+str(month_1[2]))
                    else:
                        app.append(months[str(month[1])]+":"+month[0]+" "+str(month[2]))
                        app.append(months[str(month[1])]+":"+month_1[0]+" "+str(month_1[2]))

    for elem in app:
        print("%s\t" %(elem))


ticker_year = records_rdd.mapValues(f)
ticker_name = ticker_year.join(ticker_name_month).map(lambda line:(line[0],line[1][1],line[1][0])).collect()

for elem in range(0,len(ticker_name)):
    ticker = ticker_name[elem][1]
    ticker_month = ticker_name[elem][2]
    j = elem +1

    for i in range(j, len(ticker_name)):
        ticker1 = ticker_name[i][1]
        ticker1_month = ticker_name[i][2]
        compare(ticker, ticker1, ticker_month, ticker1_month)