DROP TABLE IF EXISTS docs1;
DROP TABLE IF EXISTS docs2;
DROP TABLE IF EXISTS ticker_2017;
DROP TABLE IF EXISTS ticker_month_first_date;
DROP TABLE IF EXISTS ticker_month_last_date;
DROP TABLE IF EXISTS first_date_close_value;
DROP TABLE IF EXISTS last_date_close_value;
DROP TABLE IF EXISTS ticker_variation_month;
DROP TABLE IF EXISTS stock_price_last_date;
DROP TABLE IF EXISTS results;

CREATE TABLE docs1 (ticker STRING, open_value FLOAT, close_value FLOAT, adj_close FLOAT, low FLOAT, high FLOAT, volume FLOAT, ticker_data DATE)


ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/andrea/Desktop/Big_Data/BigData-RomaTre/Project_1/data/historical_stock_prices.csv' OVERWRITE INTO TABLE docs1;


CREATE TABLE docs2(ticker STRING,name STRING)


ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH '/home/andrea/Desktop/Big_Data/BigData-RomaTre/Project_1/src/Job3/mapred_output' OVERWRITE INTO TABLE docs2;


CREATE TABLE ticker_2017 AS
SELECT ticker, close_value, ticker_data
FROM docs1
WHERE year(ticker_data) == 2017;


CREATE TABLE ticker_month_first_date AS
SELECT ticker, min(ticker_data) AS first_date
FROM ticker_2017
group by ticker, month(ticker_data);


CREATE TABLE ticker_month_last_date AS
SELECT ticker, max(ticker_data) AS last_date
FROM ticker_2017
group by ticker, month(ticker_data);


create TABLE first_date_close_value AS
SELECT a.ticker, date_format(a.ticker_data,'MMM') AS month, a.close_value AS close_fd
FROM ticker_2017 a, ticker_month_first_date b
WHERE a.ticker = b.ticker and a.ticker_data = b.first_date;


create TABLE last_date_close_value AS
SELECT a.ticker,date_format(a.ticker_data,'MMM')AS month, a.close_value AS close_ld
FROM ticker_2017 a, ticker_month_last_date b
WHERE a.ticker = b.ticker and a.ticker_data = b.last_date;


create TABLE ticker_variation_month AS
SELECT a.ticker,c.name,a.month, (((a.close_ld-b.close_fd)/b.close_fd)*100) AS variation
FROM last_date_close_value a, first_date_close_value b, docs2 c
WHERE a.ticker = b.ticker and a.month = b.month and a.ticker = c.ticker;


create TABLE results AS
SELECT a.name AS name1,b.name AS name2,a.month AS month1,a.ticker AS ticker1 ,a.variation AS variation1 ,b.month AS month2,b.ticker AS ticker2 ,b.variation AS variation2
FROM ticker_variation_month a,ticker_variation_month b
WHERE a.ticker != b.ticker and a.month = b.month and (abs(a.variation - b.variation) >=0 and abs(a.variation - b.variation) <=1)
ORDER BY name1, name2, month1;

SELECT * FROM results LIMIT 10;
