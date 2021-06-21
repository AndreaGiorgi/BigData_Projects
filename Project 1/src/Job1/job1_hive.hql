DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS ticker_first_date;
DROP TABLE IF EXISTS ticker_last_date;
DROP TABLE IF EXISTS ticker_min_low;
DROP TABLE IF EXISTS ticker_max_high;
DROP TABLE IF EXISTS ticker_fd;
DROP TABLE IF EXISTS ticker_ld;
DROP TABLE IF EXISTS history_stock_prices;
DROP TABLE IF EXISTS result;

CREATE TABLE docs (ticker STRING,open FLOAT,close FLOAT,adj_close FLOAT,low FLOAT,high FLOAT,volume FLOAT,ticker_date DATE)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/andrea/Desktop/Big_Data/BigData-RomaTre/Project_1/data/historical_stock_prices_debug.csv' OVERWRITE INTO TABLE docs;


CREATE TABLE ticker_first_date AS
SELECT ticker,min(ticker_date) AS first_date
FROM docs
GROUP BY ticker;


CREATE TABLE ticker_last_date AS
SELECT ticker,MAX(ticker_date) AS last_date
FROM docs
GROUP BY ticker;


CREATE TABLE ticker_min_low AS
SELECT ticker,min(low) AS low
FROM docs
GROUP BY ticker;


CREATE TABLE ticker_max_high AS
SELECT ticker,MAX(high) AS high
FROM docs
GROUP BY ticker;


CREATE TABLE ticker_fd AS
SELECT a.ticker,a.open,a.close,a.low,a.high,b.first_date
FROM docs a, ticker_first_date b
WHERE a.ticker = b.ticker AND a.ticker_date = b.first_date;


CREATE TABLE ticker_ld AS
SELECT a.ticker, a.open ,a.close, a.low, a.high, b.last_date
FROM docs a, ticker_last_date b
WHERE a.ticker = b.ticker AND a.ticker_date = b.last_date;


CREATE TABLE history_stock_prices AS
select a.ticker, a.first_date, b.last_date, ((b.close-a.close)/a.close)*100 AS var
from ticker_fd a, ticker_ld b
where a.ticker = b.ticker; 


CREATE TABLE result as
select a.ticker, a.first_date, a.last_date, a.var, l.low, h.high
from history_stock_prices a, ticker_min_low l,ticker_max_high h
where a.ticker = l.ticker AND a.ticker = h.ticker
ORDER BY a.last_date DESC;

SELECT * FROM result
LIMIT 10;