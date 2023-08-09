# ETL: Multiple Database to Postgres Database Migration

this project aims to write a script which will help to fetch data from different database and store them in a single database


## Step 1: Connecting to Database

- Put all the credential in the <b>credential.json</b> file.
- This is needed to keep all the db connections and table with source name in a database. (Source database is preferable)


```
{
    "database" : "",
    "user" : "",
    "password" : "",
    "host" : "",
    "port" : "5432"
}
```

## Step 2 : generating config Tables (Important)
In order to run this script without error, following process must be followed.
 - Execute the sql file in the destination database. Create a schema named <b>conf</b> if does not exists

> config_file.sql


 - Follow configuration table instructions carefully. Otherwaise you might get error while running the script

Table Name: conf_table
---------------------------
 - This  tables keeps all the configuration required to connect with both destination and source database prior to satrt transferring data.

use this below format to insert data into this table. 

```
insert
	into
	conf.conf_table
(
	source_db,
	destination_db,
	source_credential,
	destination_credential,
	source_db_name,
	is_active,
	created_at,
	updated_at,
	added_by,
	updated_by)
values(
'sqlserver',
'postgres',
'{"host_name": "", "port_name": "", "user_name": "", "database_name": "", "user_password": ""}'::jsonb,
'{"host_name": "", "port_name": "", "user_name": "", "database_name": "", "user_password": ""}'::jsonb,
'',
false,
now(),
now(),
current_user,
current_user);
```


- To connect to two database you have to set   **is_active**   column to **true**.
- this scripst connects to thre source database now . which are Mysql,Postgres and MsSQL database. while giving input in **source_db** column the name must be like this **postgres** or **sqlserver** or **mysql**. 


** In case the json column needs to Update **

```
--All key value of a json:

UPDATE conf.conf_table SET source_credential='{"host_name": "", "port_name": "", "user_name": "", "database_name": "", "user_password": ""}' where id=table_id;

--Single Key of a json column:

UPDATE conf.conf_table 
SET source_credential = jsonb_set(source_credential, '{host_name}', '"host_ip"')

```


Table Name: etl_table_conf
---------------------------
This table contains table names of source table and corresponding destination table name. A convention is being followed in order to keep all table in destination schema. table name in destination db shourd be **databaseName__tableName** . please use this format.


```
insert into conf.etl_table_conf
(
	source_table,
	dbname,
	destination_table,
	is_active,
	is_special,
	last_data_inserted,
	has_failed,
	primary_column,
	destination_schema,
	data_insertion_type,
	is_autoincremental,
	source_db_or_schema,
	is_updateable,
	update_column,
	last_table_updated,
	chunk_size)
values('','','',false,false,now(),false,'','','',true,'',,,,);


```

- If the **is_active** is set to true then the the script will transfer data for those tables.
- **primary_column** refres to primary column name. It is only needed when data is inserted incrementally.
- schema name must be included for destination and source schema. mysql database does not contain any schema. so put database name in source schema column.
- **is_updatable** column used for those table where data is continuously updated.
- **chunk_size** value indicates how many row will be fetched every time.
-**data_insertion_type** value set to 'full' or 'partial' in order to specify how data will be transferred.


Table Name: etl_status_log
---------------------------
This table keep log of every table when the script is rua. If there is any error it records the reason of the error so that the error can be solved in future.


## Step 3 : Key Features

1. This Script can fetches data from three diffrent database. But the destination database is postgres. 

2. It handles data type issues before inserting data to postgres database. Most of the data type error cases were handled. Still there might be some error

3. The script is developed using functional programming. OOP is not used.
4, The script not only transfers data from source to destination but also updates the tables in order to keep sync with source tables.

## Step 4 : Run The script

1. Create a virtual environment if needed . (Not Mandatory)
2. Install all the library from requirements.txt using command line.
    > pip install -r requirements.txt

3. Run the main.py file.
    >python3 main.py