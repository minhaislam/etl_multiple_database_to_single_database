-- DROP TABLE conf.conf_table;

CREATE TABLE conf.conf_table (
	id serial4 NOT NULL,
	source_db text NULL,
	destination_db text NULL,
	source_credential jsonb NULL,
	destination_credential jsonb NULL,
	source_db_name text NULL,
	is_active bool NULL DEFAULT false,
	created_at timestamp(0) NOT NULL DEFAULT now(),
	updated_at timestamp(0) NOT NULL DEFAULT now(),
	added_by text NOT NULL DEFAULT SESSION_USER,
	updated_by text NOT NULL DEFAULT SESSION_USER,
	CONSTRAINT conf_table_pk PRIMARY KEY (id)
);



--DROP TABLE conf.etl_table_conf;

CREATE TABLE conf.etl_table_conf (
	id serial4 NOT NULL,
	source_table text NULL,
	dbname text NULL,
	destination_table text NULL,
	is_active bool NULL DEFAULT false,
	is_special bool NULL DEFAULT false,
	last_data_inserted timestamp(0) NOT NULL DEFAULT now(),
	has_failed bool NULL DEFAULT false,
	primary_column text NULL,
	destination_schema text NULL,
	data_insertion_type text NULL,
	is_autoincremental bool NOT NULL DEFAULT false,
	source_db_or_schema text NULL,
	is_updateable bool NOT NULL DEFAULT false,
	update_column text NULL,
	last_table_updated timestamp(0) NULL,
	chunk_size int4 NULL,
	CONSTRAINT etl_table_conf_pk PRIMARY KEY (id)
);


-- DROP TABLE conf.etl_status_log;

CREATE TABLE conf.etl_status_log (
	id serial4 NOT NULL,
	table_name text NULL,
	created_datetime timestamp(0) NULL DEFAULT now(),
	is_successful bool NULL DEFAULT false,
	message text NULL,
	CONSTRAINT etl_status_log_pk PRIMARY KEY (id)
);