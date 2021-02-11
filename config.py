from string import Template

DB_NAME = 'fred_ts'
SCHEMA_NAME = 'fin_ts'
ETL_LOG_TABLE = 'etl_log'
WAREHOUSE = 'fin_ts_wh'
#ROLE = 'FIN_TS_ADMIN'
#DB_USER = 'fred'
#PSWD = 'UPyy1G6YLE2FejSdXYBycC'
ROLE = 'ACCOUNTADMIN'
DB_USER = 'mandeep'
PSWD = 'xcxzm6YovD@f^1N%3xEI'
ACCT = 'alation_partner'

API_KEY = '840edf0dfd657ca4a7328d5a17e04598'

# ---------------------------------------------------------------------------------------------------#
## SQL Templates
# ---------------------------------------------------------------------------------------------------#
INSERT_TEMPLATE = Template("""INSERT INTO $db_name.$schema_name.$table_name ($cols) VALUES ($vals);""")

LAST_DATE_TEMPLATE = Template("""
SELECT MAX(date) FROM $schema_name.$table_name;
""")

SERIES_INSERT_TEMPLATE = Template("""
INSERT INTO $schema_name.$table_name VALUES (
    %(date)s,
    %(value)s
);
""")

TRUNCATE_TEMPLATE = Template("""
TRUNCATE TABLE $db_name.$schema_name.$table_name;
""")

### -----------------------------------------------------------------------------------------------###
## ETL Table
### -----------------------------------------------------------------------------------------------###
SCHEMA_CHK = Template("""CREATE SCHEMA IF NOT EXISTS $db_name.$schema_name;""")
ETL_TABLE_SETUP = Template("""
CREATE TABLE IF NOT EXISTS $db_name.$schema_name.$table_name (
    ts_recorded TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    success_flag BOOLEAN,
    series_id VARCHAR(128),
    log_messages ARRAY,
    items_processed INTEGER,
    total_items INTEGER,
    series_last_updated TIMESTAMP WITH TIME ZONE,
    message VARCHAR
);
""")

### -----------------------------------------------------------------------------------------------###
# Series Table
### -----------------------------------------------------------------------------------------------###
SERIES_TABLE_SETUP = Template("""
CREATE TABLE IF NOT EXISTS $db_name.$schema_name.$table_name (
    date DATE NOT NULL,
    value DOUBLE PRECISION NULL
);
""")

SERIES_INFO_TABLE = 'SERIES_INFO'
SERIES_INFO_TABLE_SETUP = Template("""
CREATE TABLE IF NOT EXISTS $db_name.$schema_name.$table_name(
id varchar,
realtime_start date,
realtime_end date,
title varchar,
observation_start date,
observation_end date,
frequency varchar,
frequency_short varchar,
units varchar,
units_short varchar,
seasonal_adjustment varchar,
seasonal_adjustment_short varchar,
last_updated timestamp,
popularity numeric,
notes varchar(3000));
""")

LAST_UPDATE_DT_TEMPLATE = Template("""
SELECT MAX(series_last_updated) FROM $db_name.$schema_name.$table_name WHERE series_id IN ('$series_id');
""")