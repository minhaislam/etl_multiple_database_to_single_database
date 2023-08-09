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

## Step2 : generating config Tables (Important)
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




Table Name: conf_table
---------------------------
This table contains
