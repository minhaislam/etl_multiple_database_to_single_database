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


def make_sperate_path_for_general_table(records):
    list_of_tuple = []
    data_list = []
    datas = records
    for data in datas:
        for each_data in data:
            if type(each_data)is list:
                data_list.append(json.dumps(each_data))
            else:
                data_list.append(each_data)

        list_of_tuple.append(tuple(data_list))
        data_list.clear()
    return list_of_tuple


def update_to_destination(**update_arguments):
    print('Primary Column Table : ',update_arguments['table_primary_column'])
    sql_query_column_data_type= f'''               
                                                
                                        select
                                column_name,
								udt_name 
                            from
                                information_schema.columns
                            where
                                table_schema = '{update_arguments['destination_schema']}'
                                and table_name = '{update_arguments['destination_table']}'
                               -- and column_name != '{update_arguments['table_primary_column']}';
                                
                                                '''
                
    print(sql_query_column_data_type)
    destination_db_cursor = update_arguments['destination_db_connection'].cursor()
    destination_db_cursor.execute(sql_query_column_data_type)
    table_info = destination_db_cursor.fetchall()

    print(table_info)
    field_names = tuple([i[0] for i in table_info])
    data_points = tuple([i[0] for i in table_info if i[0]!=update_arguments['table_primary_column']])
    # others = [i[1] for i in table_info]
    print(field_names)
    field_namess = ', '.join(field_names)
    print(type(field_namess))

    srt_value = [f'{data_points[i]}=e.{data_points[i]}' for i in range(len(data_points))]
    srt_value_1 = ', '.join(srt_value)
    print(srt_value_1)
    insert_string = f'''UPDATE {update_arguments['destination_schema']}.{update_arguments['destination_table']} AS t 
                                SET {srt_value_1} 
                                FROM (VALUES %s) AS e({field_namess})
                                WHERE e.{update_arguments['table_primary_column']} = t.{update_arguments['table_primary_column']};'''

    print(insert_string)


    try:
        print(update_arguments['rows_to_insert'])
        psycopg2.extras.execute_values (destination_db_cursor, insert_string, update_arguments['rows_to_insert'], template=None, page_size=10000)
        update_arguments['destination_db_connection'].commit()

    except ValueError as typo:
        print(typo)



def insert_to_destination(**insert_arguments):
    sql_query_column_data_type= f'''               
                                                
                                        select
                                column_name,
								udt_name 
                            from
                                information_schema.columns
                            where
                                table_schema = '{insert_arguments['destination_schema']}'
                                and table_name = '{insert_arguments['destination_table']}';
                                
                                                '''
                

    destination_db_cursor = insert_arguments['destination_db_connection'].cursor()
    destination_db_cursor.execute(sql_query_column_data_type)
    table_info = destination_db_cursor.fetchall()


    field_names = [i[0] for i in table_info]
    others = [i[1] for i in table_info]
    print(others)

    field_namess = ', '.join(field_names)

    srt_value = [f'%s::{others[i]}' for i in range(len(field_names))]
    srt_value_1 = ', '.join(srt_value)

    # res = [type(ele) for ele in insert_arguments['rows_to_insert'][0]]

    
 
# printing result
    # print("The data types of tuple in order are : " + str(res))

    insert_string = f'''insert into {insert_arguments['destination_schema']}.{insert_arguments['destination_table']} ({field_namess}) values ({srt_value_1}) '''
    # print(insert_string)
    try:
        # print(insert_arguments['rows_to_insert'])
        psycopg2.extras.execute_batch(destination_db_cursor,insert_string,make_sperate_path_for_general_table(insert_arguments['rows_to_insert']), page_size=10000)
        insert_arguments['destination_db_connection'].commit()
        status = 'Success'
        success = True
    except ValueError as typo:
        print(typo)
        rows_to_insert = handle_string_literal_character(insert_arguments['rows_to_insert'])
        psycopg2.extras.execute_batch(destination_db_cursor,insert_string,rows_to_insert)
        insert_arguments['destination_db_connection'].commit()
        status = 'Success'
        success = True

        # break
    return [status,success]




def handle_string_literal_character(v_list_of_tuple):
    final_data_set = []
    for i in v_list_of_tuple:
        v_modified_tuple = tuple(str(item).replace("\x00",' ') for item in i)
        final_data_set.append(v_modified_tuple)

    return final_data_set




def getConnection(get_credentials):
    for get_credential in get_credentials:
        print(get_credential)
        # print(get_credential['source_db'])
        get_src_connection = connect_src(get_credential)
        get_destination_connection = connect_dest(get_credential)
        table_lists = gettable_names(get_destination_connection[1],get_credential['source_db_name'])
        # print(table_list)
        for table_list in table_lists: 
            if table_list['is_autoincremental'] == True  :
                print(f"Working on Incremental table:{table_list['source_table']} ")
                print(f"{get_destination_connection[1]},{table_list['destination_table']},{table_list['destination_schema']},{table_list['primary_column']}")
                
                # table_list['chunk_size']
                # is_updateable
                if table_list['data_insertion_type'] == 'partial':
                    destination_max_id = fetch_max_id_dest(get_destination_connection[1],table_list['destination_table'],table_list['destination_schema'],table_list['primary_column'])
                    print(f"Destination Table {table_list['destination_schema']}.{table_list['destination_table']} column {table_list['primary_column']} Max Id : {destination_max_id}")
                    source_max_id = fetch_max_id_source(get_src_connection[1],table_list['source_table'],table_list['source_db_or_schema'],table_list['primary_column'])
                    print(f"Source Table {table_list['dbname']}.{table_list['source_table']} column {table_list['primary_column']} Max Id : {source_max_id}")
                    get_final_status = fetch_and_insert_data(source_connection = get_src_connection[0],source_table_name =table_list['source_table'],source_db_name = table_list['source_db_or_schema'],source_primary_column=table_list['primary_column'],source_table_last_id = source_max_id,destination_db_connection = get_destination_connection[0],destination_table_name = table_list['destination_table'],destination_schema_name = table_list['destination_schema'],destination_table_last_id=destination_max_id,fetch_data_per_loop = table_list['chunk_size'])
                    if table_list['is_updateable'] == True:
                        get_update_status = fetch_and_update_data(source_connection = get_src_connection[0],source_table_name =table_list['source_table'],source_db_name = table_list['source_db_or_schema'],source_primary_column=table_list['primary_column'],destination_db_connection = get_destination_connection[0],destination_table_name = table_list['destination_table'],destination_schema_name = table_list['destination_schema'],table_last_update_date=table_list['last_table_updated'],update_colum_name=table_list['update_column'])
                        update_status(destination_db_conn =get_destination_connection[0],destination_table_name = table_list['destination_table'])
                    else:
                        print(f'''Update not required for {table_list['destination_table']}''')

                else:
                    print('Auto Incremental But needs full refresh')
                    truncate_table(destination_db_connection = get_destination_connection[0],destination_table = table_list['destination_table'],destination_schema =   table_list['destination_schema'])
                    destination_max_id = fetch_max_id_dest(get_destination_connection[1],table_list['destination_table'],table_list['destination_schema'],table_list['primary_column'])
                    print(f"Destination Table {table_list['destination_schema']}.{table_list['destination_table']} column {table_list['primary_column']} Max Id : {destination_max_id}")
                    source_max_id = fetch_max_id_source(get_src_connection[1],table_list['source_table'],table_list['source_db_or_schema'],table_list['primary_column'])
                    print(f"Source Table {table_list['dbname']}.{table_list['source_table']} column {table_list['primary_column']} Max Id : {source_max_id}")
                    get_final_status = fetch_and_insert_data(source_connection = get_src_connection[0],source_table_name =table_list['source_table'],source_db_name = table_list['source_db_or_schema'],source_primary_column=table_list['primary_column'],source_table_last_id = source_max_id,destination_db_connection = get_destination_connection[0],destination_table_name = table_list['destination_table'],destination_schema_name = table_list['destination_schema'],destination_table_last_id=destination_max_id,fetch_data_per_loop = table_list['chunk_size'])

            else:
                # print(table_list)
                print(f"Working on Non-Incremental table:{table_list['source_table']} ")
                get_final_status = fetch_and_insert_nonincremental_data(source_connection = get_src_connection[0],source_table_name =table_list['source_table'],source_db_name = table_list['source_db_or_schema'],destination_db_connection = get_destination_connection[0],destination_table_name = table_list['destination_table'],destination_schema_name = table_list['destination_schema'])
            
            keep_log(destination_db_conn =get_destination_connection[0],log_table = table_list['destination_table'],successful_status = get_final_status[1],insertion_status = get_final_status[0])
            



def update_status(**log_details):
    destination_db_cursor = log_details['destination_db_conn'].cursor()
    # destination_db_cursor.execute(sql_query_column_data_type)
    insert_string = f'''update test.etl_table_conf
                            set last_table_updated = current_timestamp 
                            where destination_table ='{log_details['destination_table_name']}'; '''
    print(insert_string)
    destination_db_cursor.execute(insert_string)
    log_details['destination_db_conn'].commit()


def keep_log(**log_details):
    destination_db_cursor = log_details['destination_db_conn'].cursor()
    # destination_db_cursor.execute(sql_query_column_data_type)
    insert_string = f'''insert into test.etl_status_log (table_name,is_successful,message) values (%s,%s,%s) '''
    print(insert_string)
    print([({log_details['log_table']},{log_details['successful_status']},{log_details['insertion_status']})])
    psycopg2.extras.execute_batch(destination_db_cursor,insert_string,[(log_details['log_table'],log_details['successful_status'],log_details['insertion_status'])])
    log_details['destination_db_conn'].commit()


if __name__ == '__main__':
    get_credentials = getConnectionCredentials() # fetch Database Configurations
    getConnection(get_credentials) # fetch D