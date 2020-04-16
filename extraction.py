from tqdm import tqdm
from config import *
from support_funcs import *
from time import sleep

for each in tqdm(SERIES):
    tfd = fred_datum(each)
    out_log = tfd.process_data()

    e_query = etl_log_insert_template.substitute(schema_name = tfd.schema_name,
                                                 table_name = ETL_LOG_TABLE,
                                                 serial_id = tfd.name,
                                                 log_message = out_log)

    try:
        connect_and_execute(e_query)
    except Exception:
        print(Exception)

# update yield curve data
all_dates,full_query = yc_refresh()
connect_and_execute(all_dates)
connect_and_execute(full_query)