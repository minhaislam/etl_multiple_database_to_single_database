# ETL: Multiple Database to Postgres Database Migration

this project aims to write a script which will help to fetch data from different database and store them in a single database


## Step 1:

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

