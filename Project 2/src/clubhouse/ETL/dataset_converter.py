import sqlite3 as sql
from sqlite3 import Error
import pandas as pd
import time

def dataset_export():
    
    try:
        
        start_time = time.time()
        
        db = sql.connect('src/clubhouse/ETL/Clubhouse_Dataset_v6.db')
    
        users_dataset = pd.read_sql_query("SELECT * FROM user", db)
        clubs_dataset = pd.read_sql_query("SELECT * FROM club", db)
        
        users_dataset.to_csv('data/clubhouse/user_data.csv', index = False, encoding = 'utf-8', line_terminator='\n',  quoting=1)
        clubs_dataset.to_csv('data/clubhouse/club_data.csv', index = False, encoding = 'utf-8', line_terminator='\n', quoting=1)
        
        users_dataset.to_json('data/clubhouse/user_data.json')
        clubs_dataset.to_json('data/clubhouse/club_data.json')
        
        print("--- Dataset loaded in %s seconds ---" % (time.time() - start_time))
        
    except Error as e:
        print(e)

def loading_pipeline():
    dataset_export()
    
if __name__=="__main__":
    loading_pipeline()
    