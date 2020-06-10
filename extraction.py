from tqdm import tqdm
from config import *
from support_funcs import *

# ensure ETL table exists
ETL_table_check()

# open tickers
with open('tickers.dat','r') as f:
    tickers = f.readlines()

tickers = list(map(lambda x: x.replace('\n',''),tickers))

all_tickers = SERIES + tickers
i = 0
# loop through each series
for each in all_tickers:
    i = i + 1
    if i % 10 == 0 and i > 1:
        print('Working on series number {} of {}'.format(i.len(all_tickers)))
    try:
        cur_series = fred_datum(each)
        # if we don't have the given time-series, then we must get it
        if not cur_series.exists_in_db:
            cur_series.get_data()
            cur_series.db_check()
            res = cur_series.connect_and_load()
            process_log(res)

        else:
            last_updated_by_fred = cur_series.get_last_updated()
            # if there is a new release available, then get the data
            if last_updated_by_fred > cur_series.series_last_updated:
                cur_series.get_data()
                res = cur_series.connect_and_load()
                process_log(res)
    except:
        print("could not process series {}".format(each))