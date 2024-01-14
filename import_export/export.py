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

def read_profile(profile_path:Path) -> Dict[str, str]:
    tab_key_value = list()
    if profile_path.exists():
        with open(profile_path.absolute(), 'r') as file:
           if db_ids := file.readlines():
               db_ids = db_ids[3:len(db_ids)-1]
               for value in db_ids:
                    key, value = value.split(':')[0].strip(), value.split(':')[1].removesuffix('\n').strip()
                    tab_key_value.append((key, value))
    return dict(tab_key_value)
                   
def close_connection(db_conn:Any = None, cursor:Any = None) -> None:
    if db_conn is not None:
        db_conn.close()
    if cursor is not None:
        cursor.close()

def test_connection(db_ids:Dict[str, str], db_type:str='postgresql') -> None:
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


def extract_data(db_ids:Dict[str, str], table_name:str = None) -> None:
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

def list_table(db_ids:Dict[str, str], batch_size:int=None) -> None:
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

def bulk_create(db_ids:Dict[str, str], batch_size:int=None, table_name:str=None, data:List[Tuple]=[]) -> None:
    conn, cursor = test_connection(db_ids)
    assert batch_size is None or batch_size > 0 
    query = f""" select column_name \
            from information_schema.columns \
            where table_name = '{table_name}'
        """
    cursor.execute(query)
    query_result = cursor.fetchall()[batch_size:]
    query_result = tuple(column_name[0] for column_name in query_result)
    value = '?' * len(query_result)
    value = ','.join(value)
    tuple_format = '(' + ",".join(query_result) + ')'
    insert_query = """ INSERT INTO {}{} VALUES({}) """.format(table_name, tuple_format,value.replace("?", "%s"))
    cursor.execute(insert_query, data)
    conn.commit()
    close_connection(db_conn=conn, cursor=cursor)


        
if __name__ == '__main__':
    data = read_profile(Path(HOME+ '/.dbt/profiles.yml'))
    #bulk_create(db_ids=data, table_name="countries", data=('NG','Nigeria'))
    bulk_create(db_ids=data, table_name="people", data=(6,'ivan@gmail.com','ivan','Paris', 'France', 'FR'))
    #extract_data(db_ids=data, table_name="people")
    #extract_data(db_ids=data, table_name="countries")
    #extract_data(db_ids=data, table_name="invoices")

"""
- possible to make quit insertion (insertion en masse)
- possible to delete more data (suppression en masse)
"""