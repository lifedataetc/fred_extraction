from string import Template

DB_NAME = 'fred_ts'
SCHEMA_NAME = 'fin_ts'
ETL_LOG_TABLE = 'etl_log'

CONNECTION_STRING = "dbname=fred_ts host=localhost user=fred password=180389"

API_KEY = '840edf0dfd657ca4a7328d5a17e04598'

SERIES = [
    'DGS1MO','DGS3MO','DGS6MO','DGS1','DGS2',
    'DGS3','DGS5','DGS7','DGS10','DGS20','DGS30'
        ]

# SQL templates
table_check_template = Template("""
SELECT EXISTS (
   SELECT FROM information_schema.tables
   WHERE  table_schema = '$schema_name'
   AND    table_name   = '$table_name'
   );
""")

last_date_template = Template("""
SELECT MAX(date) FROM $schema_name.$table_name;
""")

insert_template = Template("""
INSERT INTO $schema_name.$table_name VALUES (
    %(realtime_start)s,
    %(date)s,
    %(value)s
);
""")

drop_template = Template("""
DROP TABLE IF EXISTS $schema_name.$table_name;
""")

create_template = Template ("""
CREATE TABLE $schema_name.$table_name (
    realtime_start DATE NOT NULL,
    date DATE NOT NULL,
    value DOUBLE PRECISION NULL
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA $schema_name TO fred;
""")

etl_log_insert_template = Template("""
INSERT INTO $schema_name.$table_name (series_id,log_message) VALUES ('$serial_id','$log_message');
""")