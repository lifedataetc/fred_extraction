# FRED Time Series Data Collection

This code extracts time series data from FRED and uploads it to a PostgreSQL

## ETL Table Definition
```sql
CREATE TABLE fin_ts.etl_log (
    ts_recorded TIMESTAMP WITHOUT TIME zone DEFAULT (NOW() AT TIME zone 'utc'),
    series_id VARCHAR(128),
    log_message VARCHAR(1000)
);
GRANT ALL PRIVILEGES ON SCHEMA fin_ts TO fred;
```
