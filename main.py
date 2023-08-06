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