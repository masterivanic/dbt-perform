from pathlib import Path
from psycopg2 import connect
from psycopg2.errors import UndefinedTable
from psycopg2 import DatabaseError
from pathlib import Path
from typing import Dict
from typing import Any
import csv
import os

HOME = os.getenv('HOME', None)
BASE_DIR = Path(__file__).resolve().parent.parent

def read_profile(profile_path:Path) -> Dict:
    tab_key_value = list()
    if profile_path.exists():
        with open(profile_path.absolute(), 'r') as file:
           if db_ids := file.readlines():
               db_ids = db_ids[3:len(db_ids)-1]
               for value in db_ids:
                    key, value = value.split(':')[0].strip(), value.split(':')[1].removesuffix('\n').strip()
                    tab_key_value.append((key, value))
    return dict(tab_key_value)
                   
def close_connection(db_conn:Any = None, cursor:Any = None):
    if db_conn is not None:
        db_conn.close()
    if cursor is not None:
        cursor.close()

def test_connection(db_ids:Dict[str, str], db_type:str='postgresql'):
    try:
        if db_type == 'postgresql':
            ids = {
                'database': db_ids['dbname'],
                'user': db_ids['user'],
                'password': db_ids['pass'],
                'host': '127.0.0.1' if db_ids['host'] == 'localhost' else db_ids['host'],
                'port': db_ids['port']
            }
            db_conn = connect(**ids)
            cursor = db_conn.cursor()
            if db_conn:
                return db_conn, cursor
    except(ConnectionError, DatabaseError) as error:
        print(error, "====> Error")

def extract_data(db_ids:Dict[str, str], table_name:str = None):
    conn, cursor = test_connection(db_ids)
    try:
        table_name_query = 'select column_name \
                            from information_schema.columns \
                            where table_name = %s'
        query = f'select * from {table_name}'
        cursor.execute(table_name_query, (table_name,))
        table_query_name_result = cursor.fetchall()
        cursor.execute(query)
        query_result = cursor.fetchall()
        close_connection(db_conn=conn, cursor=cursor)
        default_file_path = os.path.join(BASE_DIR, f'seeds/{table_name}.csv')
        with open(default_file_path, 'w+', newline='') as csvfile:
            data_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            data_writer.writerow([data[0] for data in table_query_name_result])

            for data in query_result:
                data_writer.writerow(data)

            print("data extracted successfully ==> ")
    except UndefinedTable as error:
        print(error)
   
        
if __name__ == '__main__':
    data = read_profile(Path(HOME+ '/.dbt/profiles.yml'))
    extract_data(db_ids=data, table_name="people")
    extract_data(db_ids=data, table_name="countries")