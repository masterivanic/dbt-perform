from pathlib import Path
from psycopg2 import connect
from psycopg2.errors import UndefinedTable
from psycopg2 import DatabaseError
from pathlib import Path
from typing import Dict
from typing import Any
from typing import Tuple
from typing import List
import time
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


def write_data(data:List[Tuple], batch_size:int, data_size:int, start:int, end:int):
    if data_size > batch_size:
        data_size = data_size - batch_size 
        end = batch_size
        for i in range(start, end):
            yield data[i]
        data = data[end:]
        end = len(data)
        yield from write_data(
            data=data, 
            batch_size=batch_size, 
            data_size=data_size, 
            start=start, 
            end=end
        )
    if data_size <= batch_size:
        for i in range(start, data_size):
            yield data[i]
            

        
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
            data_writer.writerow((data[0] for data in table_query_name_result))

            time_started = time.time()
            for data in write_data(data=query_result, batch_size=6, data_size=len(query_result),start=0, end=len(query_result)):
                data_writer.writerow(data)
            final_time = time.time() - time_started
            print(f"the process take: {final_time}")
            print("data extracted successfully ==> ")
    except UndefinedTable as error:
        print(error)

def list_table(db_ids:Dict[str, str], batch_size:int=None):
    conn, cursor = test_connection(db_ids)
    query = f""" select table_name \
            from information_schema.tables\
            where table_type = 'BASE TABLE' \
            and table_schema = 'public'
        """
    cursor.execute(query)
    query_result = cursor.fetchall()[batch_size:]
    print(query_result)
    close_connection(db_conn=conn, cursor=cursor)
        
if __name__ == '__main__':
    data = read_profile(Path(HOME+ '/.dbt/profiles.yml'))
    list_table(db_ids=data)
    #extract_data(db_ids=data, table_name="people")
    #extract_data(db_ids=data, table_name="countries")
    #extract_data(db_ids=data, table_name="invoices")