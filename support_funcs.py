import datetime as dt
import psycopg2 as pg
from fredapi import Fred
from string import Template
from config import *


# ---------------------------------------------------------------------------------------------------#
## SQL Functions
# ---------------------------------------------------------------------------------------------------#
def execute_q(query,conn_str=CONNECTION_STRING):
    """simple function to connect and execute a query"""
    conn = pg.connect(conn_str)
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()
    conn.close()
    return([True,None])

def select_data(query,con_str=CONNECTION_STRING):
    """simple function to connect and select data"""
    conn = pg.connect(con_str)
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return(data)

# ---------------------------------------------------------------------------------------------------#
## Utility Functions
# ---------------------------------------------------------------------------------------------------#
# insert template maker
def insert_query_maker(table_name,cols):
    vals = ','.join(['%s']*len(cols))
    cols_str = ','.join(cols)
    temp = INSERT_TEMPLATE.substitute(schema_name=SCHEMA_NAME,table_name=table_name,
                                      cols=cols_str,vals=vals)
    return(temp)

def process_log(res):
    """simple function to connect and load data for ETL log"""
    cols = list(res.keys())
    vals = tuple(res.values())
    temp = insert_query_maker(ETL_LOG_TABLE,cols)

    conn = pg.connect(CONNECTION_STRING)
    cur = conn.cursor()
    cur.execute(temp,vars=vals)
    conn.commit()
    cur.close()
    conn.close()
# ---------------------------------------------------------------------------------------------------#
## Logging
# ---------------------------------------------------------------------------------------------------#
def ETL_table_check():
    qry = ETL_TABLE_SETUP.substitute(schema_name=SCHEMA_NAME,table_name=ETL_LOG_TABLE,user=DB_ANALYST_USER)
    flag,err = execute_q(qry)

# ---------------------------------------------------------------------------------------------------#
## Time-series class
# ---------------------------------------------------------------------------------------------------#
class fred_datum:
    def __init__(self,name):
        self.name  = name
        self.schema_name = SCHEMA_NAME
        # ensure table exists for this series
        self.db_check()
        # get the last update information available
        self.exists_in_db, self.series_last_updated = self.last_datum()

    # fucntion to check if table exists for this data set
    def db_check(self):
        query = SERIES_TABLE_SETUP.substitute(schema_name=self.schema_name,table_name=self.name,user=DB_ANALYST_USER)
        execute_q(query)

    def get_last_updated(self):
        fred = Fred(api_key=API_KEY)
        series_info = fred.get_series_info(self.name)
        return(dt.datetime.fromisoformat(series_info.last_updated + ':00'))

    # function to check what the last update was for this data set
    def last_datum(self):
        query = LAST_UPDATE_DT_TEMPLATE.substitute(schema_name=self.schema_name,
                                                   table_name=ETL_LOG_TABLE,
                                                   series_id=self.name)
        data = select_data(query)

        # if the return is null, then series has not been extracted
        if not data[0][0]:
            return([False,self.get_last_updated()])
        else:
            return([True,data[0][0]])

    # get the newest series data
    def api_get_data(self):
        fred = Fred(api_key=API_KEY)
        data = fred.get_series_all_releases(series_id=self.name)
        data.dropna(inplace=True)
        data['realtime_start'] = data.apply(lambda x: str(x.realtime_start.date()),axis=1)
        data['date'] = data.apply(lambda x: str(x.date.date()),axis=1)

        return(data)

    def get_data(self):
        self.data = self.api_get_data()

    def connect_and_load(self):
        """simple function to connect and load data"""
        # number of entries successfully processed
        data = self.data.to_dict(orient='records')
        d_len = len(data)
        i = 0
        error_log = []
        # truncate data before uploading the new data
        qry = TRUNCATE_TEMPLATE.substitute(schema_name=SCHEMA_NAME,table_name=self.name.lower())
        execute_q(qry)
        try:
            # process each entry produced from the process above
            for each in data:
                cols = list(each.keys())
                vals = tuple(each.values())
                temp = insert_query_maker(self.name.lower(),cols)

                try:
                    # connect to the system
                    conn = pg.connect(CONNECTION_STRING)
                    cur = conn.cursor()
                    cur.execute(temp,vars=vals)
                    i += 1
                    conn.commit()
                    cur.close()
                    conn.close()
                # some basic error handling
                except pg.Error as err:
                    error_log.append(str(err))
                    try:
                        conn.rollback()
                        cur.close()
                        conn.close()
                    except:
                        pass
                except Exception as err:
                    error_log.append(str(err))
                    try:
                        conn.rollback()
                        cur.close()
                        conn.close()
                    except:
                        pass

            #res_flag, error_message, i, error_log
            return({'success_flag':True, 'error_message':None, 'items_processed':i,
                    'log_messages':error_log, 'series_id':self.name,'total_items':d_len,
                    'series_last_updated':self.series_last_updated})

        except Exception as err:
            return({'success_flag':False, 'error_message':str(err), 'items_processed':i,
                    'log_messages':error_log, 'series_id':self.name,'total_items':d_len,
                    'series_last_updated':self.series_last_updated})