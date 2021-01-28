from support_funcs import *
from config import *

sn = Snow()

# ensure ETL table exists
# qry = ETL_TABLE_SETUP.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=ETL_LOG_TABLE)
# sn.tb_check(qry)

# open tickers
with open('tickers.dat','r') as f:
    tickers = f.readlines()

all_tickers = list(map(lambda x: x.replace('\n',''),tickers))

i = 0
# loop through each series
for each in all_tickers:
    i = i + 1
    if i % 10 == 0 and i > 1:
        print('{}: Working on series number {} of {}'.format(str(datetime.now()),i,len(all_tickers)))
        
    try:
        cur_series = FredDatum(each)
        res = cur_series.update_series_db(sn)
    except Exception as e:
        print('Series {} had an error {}'.format(each,str(e)))