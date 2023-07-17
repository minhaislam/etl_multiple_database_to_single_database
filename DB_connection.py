import psycopg2
import mysql.connector
from sqlalchemy import create_engine
# import sqlalchemy
import json 

import os
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

file_name = 'credentials.json'
open_file =  open(file_name)
load_credential =json.load(open_file)

try:
    #dwh
    database = load_credential['database']
    user = load_credential['user']
    password = load_credential['password']
    host = load_credential['host']
    port = load_credential['port']
    #destination database
    postgres_connection = psycopg2.connect(database = database,
                                    user = user,
                                    password = password,
                                    host= host,
                                    port = port,
                                    
                                    )
   
    

    postgres_connection_cursor = postgres_connection.cursor()
   

    print("success")
except Exception as e:
    print(e)