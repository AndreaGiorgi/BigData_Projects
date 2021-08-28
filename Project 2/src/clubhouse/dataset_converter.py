import sqlite3 as sql
from sqlite3 import Error
import csv
import json


def dataset_to_json():
    
    try:
        
        # Load clubouse dataset
        conn = sql.connect('src\clubhouse\Clubhouse_Dataset_v6.db')
        
        # Export club data to .json
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM club")
        
        with open("data\clubhouse\club_data.json", "w+", encoding="utf-8") as json_file:
            json.dump(cursor.fetchall(), json_file, ensure_ascii=False)
            
        # Export user data to .json
        cursor.execute("SELECT * FROM user")
        
        with open("data\clubhouse\/user_data.json", "w+", encoding="utf-8") as json_file:
            json.dump(cursor.fetchall(), json_file, ensure_ascii=False)
                 
    except Error as e:
        print(e)
        
    finally:
        conn.close()

def dataset_to_csv():
    
    try:
        # Load clubouse dataset
        conn = sql.connect('src\clubhouse\Clubhouse_Dataset_v6.db')
        
        # Export club data to .csv
        
        cursor = conn.cursor()
        cursor.execute("select * from club")
        with open("data\clubhouse\club_data.csv", "w+", encoding="utf-8") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter = "\t")
            csv_writer.writerow([i[0] for i in cursor.description])
            csv_writer.writerows(cursor)
        
        # Export user data to .csv
        
        cursor.execute("select * from user")
        with open("data\clubhouse\/user_data.csv", "w+", encoding="utf-8") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter = "\t")
            csv_writer.writerow([i[0] for i in cursor.description])
            csv_writer.writerows(cursor)
            
    except Error as e:
        print(e)
        
    finally:
        conn.close()

    
def main():
    
    dataset_to_csv()
    dataset_to_json()
    
if __name__=="__main__":
    main()