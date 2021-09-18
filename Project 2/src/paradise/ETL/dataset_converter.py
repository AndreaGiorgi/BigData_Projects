import sqlite3 as sql
from sqlite3 import Error
import pandas as pd
import time
import csv
import json


def dataset_export():
    
    try:
        
        start_time = time.time()
        
        db = sql.connect('src/clubhouse/ETL/Clubhouse_Dataset_v6.db')
    
        users_dataset = pd.read_sql_query("SELECT * FROM user", db)
        clubs_dataset = pd.read_sql_query("SELECT * FROM club", db)
        
        users_dataset.to_csv('data/clubhouse/user_data.csv', index = False, encoding = 'utf-8', line_terminator='\n',  quoting=1)
        clubs_dataset.to_csv('data/clubhouse/club_data.csv', index = False, encoding = 'utf-8', line_terminator='\n', quoting=1)
                
        with open('data/clubhouse/user_data.csv', encoding = "utf-8") as csvFile:
            with open('data/clubhouse/user_data.json', 'w+', encoding='utf-8') as jsonf:
                csvReader = csv.DictReader(csvFile)
                for row in csvReader:
                    json.dump(row, jsonf)
                    jsonf.write('\n')
            
        
        with open('data/clubhouse/club_data.csv', encoding = 'utf-8') as csvFile:
            with open('data/clubhouse/club_data.json', 'w+', encoding='utf-8') as jsonf:
                csvReader = csv.DictReader(csvFile)
                for row in csvReader:
                    json.dump(row, jsonf)
                    jsonf.write('\n')
            
        print("--- Dataset loaded, saved in CSV and JSON format in %s seconds ---" % (time.time() - start_time))
 
        
    except Error as e:
        print(e)

def loading_pipeline():
    dataset_export()
    
if __name__=="__main__":
    loading_pipeline()
    