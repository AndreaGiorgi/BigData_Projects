DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS ticker_sector;
DROP TABLE IF EXISTS ticker_data;
DROP TABLE IF EXISTS stock_last_dates;
DROP TABLE IF EXISTS stock_first_dates;
DROP TABLE IF EXISTS stock_volumes;
DROP TABLE IF EXISTS stock_info;
DROP TABLE IF EXISTS stock_price_first_date;
DROP TABLE IF EXISTS stock_price_last_date;
DROP TABLE IF EXISTS variations;
DROP TABLE IF EXISTS stock_values;
DROP TABLE IF EXISTS stock_maxvar;
DROP TABLE IF EXISTS results;


CREATE TABLE historical_stock_prices (ticker STRING, open_value FLOAT, close_value FLOAT, adj FLOAT, low FLOAT, high FLOAT, volume INT, ticker_date DATE)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE ticker_sector (ticker STRING, sector STRING)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH '/home/andrea/Desktop/Big_Data/BigData-RomaTre/Project_1/data/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;
LOAD DATA LOCAL INPATH '/home/andrea/Desktop/Big_Data/BigData-RomaTre/Project_1/src/Job2/mapred_output' OVERWRITE INTO TABLE ticker_sector;


CREATE TABLE ticker_data AS
    SELECT sector, h.ticker, close_value, volume, ticker_date
    FROM historical_stock_prices h JOIN ticker_sector s ON h.ticker = s.ticker
    WHERE ticker_date BETWEEN '2009-01-01' AND '2018-12-31';


CREATE TABLE stock_last_dates AS
    SELECT ticker, max(ticker_date) AS last_date
    FROM ticker_data
    GROUP BY ticker, year(ticker_date);


CREATE TABLE stock_first_dates AS
    SELECT ticker, min(ticker_date) AS first_date
    FROM ticker_data
    GROUP BY ticker, year(ticker_date);


CREATE TABLE stock_volumes AS
    SELECT sector, ticker, sum(volume) AS volume_year, year(ticker_date) as stock_year
    FROM ticker_data
    WHERE ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
    GROUP BY ticker, year(ticker_date), sector;


CREATE TABLE stock_price_first_date AS
    SELECT t.ticker, t.close_value AS first_date_close_value, f.first_date
    FROM ticker_data t JOIN stock_first_dates f ON t.ticker = f.ticker AND t.ticker_date = f.first_date;


CREATE TABLE stock_price_last_date AS
    SELECT t.ticker, t.close_value AS last_date_close_value, l.last_date
    FROM ticker_data t JOIN stock_last_dates l ON t.ticker = l.ticker AND t.ticker_date = l.last_date;


CREATE TABLE variations AS
    SELECT l.ticker, (((last_date_close_value - first_date_close_value)/first_date_close_value)*100) AS var, first_date_close_value, last_date_close_value, year(l.last_date) AS stock_year
    FROM stock_price_last_date l JOIN stock_price_first_date f ON l.ticker = f.ticker AND year(l.last_date) = year(f.first_date);


CREATE TABLE stock_values AS
    SELECT vol.sector, s_var.ticker, s_var.var, first_date_close_value, last_date_close_value, volume_year, s_var.stock_year
    FROM variations s_var JOIN stock_volumes vol ON s_var.ticker = vol.ticker AND s_var.stock_year = vol.stock_year;


CREATE TABLE stock_info AS
    SELECT s.sector, max(var) AS maxvar, max(volume_year) AS volume, sum(first_date_close_value) as sum_first_date_close_value, sum(last_date_close_value) as sum_last_date_close_value, stock_year
    FROM stock_values s JOIN ticker_sector t ON s.ticker = t.ticker AND s.sector = t.sector
    GROUP BY s.sector, stock_year;


CREATE TABLE stock_maxvar AS
    SELECT sector, ticker, maxvar, volume, ((sum_last_date_close_value - sum_first_date_close_value)/sum_first_date_close_value)*100 as year_var, si.stock_year
    FROM stock_info si JOIN variations v ON si.maxvar = v.var AND si.stock_year = v.stock_year;

CREATE TABLE results AS
    SELECT sm.sector, sm.ticker, maxvar, volume, year_var, sm.stock_year
    FROM stock_maxvar sm JOIN stock_volumes v ON sm.volume = v.volume_year AND sm.sector = v.sector AND sm.stock_year = v.stock_year
    ORDER BY sm.sector, sm.stock_year;

SELECT * FROM results LIMIT 10;