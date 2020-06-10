from string import Template
# ---------------------------------------------------------------------------------------------------#
## DB + API Settings & Information
# ---------------------------------------------------------------------------------------------------#
DB_NAME = 'fred_ts'
DB_ANALYST_USER = 'fred'
SCHEMA_NAME = 'fin_ts'
ETL_LOG_TABLE = 'etl_log'

CONNECTION_STRING = "dbname=fred_ts host=localhost user=fred password=180389"

API_KEY = '840edf0dfd657ca4a7328d5a17e04598'

SERIES = [
    'DGS1MO','DGS3MO','DGS6MO','DGS1','DGS2',
    'DGS3','DGS5','DGS7','DGS10','DGS20','DGS30',
    'JHDUSRGDPBR','FEDFUNDS'
        ]

# ---------------------------------------------------------------------------------------------------#
## SQL Templates
# ---------------------------------------------------------------------------------------------------#
INSERT_TEMPLATE = Template("""INSERT INTO $schema_name.$table_name ($cols) VALUES ($vals);""")

LAST_DATE_TEMPLATE = Template("""
SELECT MAX(date) FROM $schema_name.$table_name;
""")

SERIES_INSERT_TEMPLATE = Template("""
INSERT INTO $schema_name.$table_name VALUES (
    %(realtime_start)s,
    %(date)s,
    %(value)s
);
""")

TRUNCATE_TEMPLATE = Template("""
TRUNCATE TABLE $schema_name.$table_name;
""")

### -----------------------------------------------------------------------------------------------###
## ETL Table
### -----------------------------------------------------------------------------------------------###
ETL_TABLE_SETUP = Template("""
CREATE SCHEMA IF NOT EXISTS $schema_name;
CREATE TABLE IF NOT EXISTS $schema_name.$table_name (
    ts_recorded TIMESTAMP WITHOUT TIME zone DEFAULT (NOW() AT TIME zone 'utc'),
    success_flag BOOLEAN,
    series_id VARCHAR(128),
    log_messages CHARACTER VARYING ARRAY,
    items_processed INTEGER,
    total_items INTEGER,
    series_last_updated TIMESTAMP WITH TIME ZONE,
    error_message VARCHAR
);
GRANT USAGE ON SCHEMA $schema_name TO $user;
GRANT SELECT ON ALL TABLES IN SCHEMA $schema_name TO $user;
CREATE INDEX IF NOT EXISTS etl_series_last_updated ON $schema_name.$table_name(series_last_updated);
CREATE INDEX IF NOT EXISTS etl_series_id ON $schema_name.$table_name(series_id);
""")

### -----------------------------------------------------------------------------------------------###
# Series Table
### -----------------------------------------------------------------------------------------------###
SERIES_TABLE_SETUP = Template("""
CREATE SCHEMA IF NOT EXISTS $schema_name;
CREATE TABLE IF NOT EXISTS $schema_name.$table_name (
    realtime_start DATE NOT NULL,
    date DATE NOT NULL,
    value DOUBLE PRECISION NULL
);
GRANT USAGE ON SCHEMA $schema_name TO $user;
GRANT SELECT ON ALL TABLES IN SCHEMA $schema_name TO $user;
""")

LAST_UPDATE_DT_TEMPLATE = Template("""
SELECT MAX(series_last_updated) FROM $schema_name.$table_name WHERE series_id IN ('$series_id');
""")