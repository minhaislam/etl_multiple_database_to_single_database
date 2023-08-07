import psycopg2
import psycopg2.extras
import mysql.connector
from sqlalchemy import create_engine
import math
import pymssql
from psycopg2.extensions import Binary
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)
from DB_connection import postgres_connection
import datetime
from datetime import timedelta
import json
postgres_connection_cursor = postgres_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

def getConnectionCredentials():
    sql_query = 'select * from test.conf_table where is_active is true order by id asc;'
    postgres_connection_cursor.execute(sql_query)
    get_result = postgres_connection_cursor.fetchall()
    postgres_connection.close()
    return get_result

def gettable_names(dest_cursor,source_db_name):
    sql_query = f"select * from test.etl_table_conf where is_active =true and dbname ='{source_db_name}' order by id asc;"
    dest_cursor.execute(sql_query)
    get_tables = dest_cursor.fetchall()
    return get_tables




def fetch_max_id_dest(destination_db_cursor,destination_table_name,destination_schema_name,destination_target_column):
    print('Fetching data from Destination....')
    print(f"{destination_db_cursor},{destination_table_name},{destination_schema_name},{destination_target_column}")
    sql = f"SELECT COALESCE(max({destination_target_column}),0) as {destination_target_column} FROM {destination_schema_name}.{destination_table_name}; "    
    print(sql)
    destination_db_cursor.execute(sql)   
    max_id = destination_db_cursor.fetchone()
    print('Max ID',max_id)
    return max_id[f'{destination_target_column.lower()}']




def fetch_max_id_source(source_db_cursor,source_table_name,sourc_schema_name,source_target_column):
    print('Fetching data from source....')
    sql = f"SELECT COALESCE(max({source_target_column}),0) as {source_target_column} FROM {sourc_schema_name}.{source_table_name}; " 
    print(sql)
    source_db_cursor.execute(sql)   
    max_id_2 = source_db_cursor.fetchone()
    print('Max ID2: ',max_id_2)
    return max_id_2[f'{source_target_column}']




def connect_src(src_connection):
    source_connection = ''
    source_cursor = ''
    if src_connection['source_db'] == 'mysql':
        try:
            source_connection = mysql.connector.connect(
                                                    host=src_connection['source_credential']['host_name'],
                                                    user=src_connection['source_credential']['user_name'],
                                                    password=src_connection['source_credential']['user_password']
                                                    )

            source_cursor = source_connection.cursor(dictionary=True)
            print('mysql connection successful')
        except Exception as e:
            print(e)
    elif src_connection['source_db'] == 'sqlserver':
        try:    
            # print('Src Database:',src_connection['source_credential']['database_name'])
            # print('Src User:',src_connection['source_credential']['user_name'])
            # print('Src Pass:',src_connection['source_credential']['user_password'])
            # print('Src Host:',src_connection['source_credential']['host_name'])
            # print('Src Port:',src_connection['source_credential']['port_name'])
                
            source_connection = pymssql.connect(database = src_connection['source_credential']['database_name'],
                                            user = src_connection['source_credential']['user_name'],
                                            password = src_connection['source_credential']['user_password'],
                                            host= src_connection['source_credential']['host_name'],
                                            port = src_connection['source_credential']['port_name']
                                            
                                                )
            
            source_cursor = source_connection.cursor(as_dict=True)
            print('MSSQL Source connection successful')
        except Exception as e:
            print(e)
    elif src_connection['source_db'] == 'postgres':
        try:    
            # print('Src Database:',src_connection['source_credential']['database_name'])
            # print('Src User:',src_connection['source_credential']['user_name'])
            # print('Src Pass:',src_connection['source_credential']['user_password'])
            # print('Src Host:',src_connection['source_credential']['host_name'])
            # print('Src Port:',src_connection['source_credential']['port_name'])
            source_connection = psycopg2.connect(
                                                    database = src_connection['source_credential']['database_name'],
                                                    user = src_connection['source_credential']['user_name'],
                                                    password = src_connection['source_credential']['user_password'],
                                                    host= src_connection['source_credential']['host_name'],
                                                    port = src_connection['source_credential']['port_name'],
                                                )

            source_cursor = source_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            print('Postgres Source connection successful')
        except Exception as e:
            print(e)
    return [source_connection,source_cursor]



def connect_dest(dest_connection):
    destination_connection = ''
    destination_cursor = ''
    if dest_connection['destination_db'] == 'postgres':
        try:
            destination_connection = psycopg2.connect(
                                                    database = dest_connection['destination_credential']['database_name'],
                                                    user = dest_connection['destination_credential']['user_name'],
                                                    password = dest_connection['destination_credential']['user_password'],
                                                    host= dest_connection['destination_credential']['host_name'],
                                                    port = dest_connection['destination_credential']['port_name'],
                                                )

            destination_cursor = destination_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            print('postgres DWH connection successful')
        except Exception as e:
            print(e)
    elif dest_connection['destination_db'] == 'sqlserver':
        try:
                destination_connection = pymssql.connect(
                                                        database = dest_connection['destination_credential']['database_name'],
                                                        user = dest_connection['destination_credential']['user_name'],
                                                        password = dest_connection['destination_credential']['user_password'],
                                                        host= dest_connection['destination_credential']['host_name'],
                                                        port = dest_connection['destination_credential']['port_name'],
                                                    )

                destination_cursor = destination_connection.cursor(as_dict=True)
                print('MSSQL DWH connection successful')
        except Exception as e:
            print(e)
    return [destination_connection,destination_cursor]


def truncate_table(**truncate_arguments):
    destination_db_cursor = truncate_arguments['destination_db_connection'].cursor()
    insert_string = f'''truncate {truncate_arguments['destination_schema']}.{truncate_arguments['destination_table']}; '''
    print(insert_string)
    destination_db_cursor.execute(insert_string)
    truncate_arguments['destination_db_connection'].commit()

if __name__ == '__main__':
    get_credentials = getConnectionCredentials() # fetch Database Configurations
    getConnection(get_credentials) # fetch D