import pandas as pd
from fredapi import Fred
from string import Template
import datetime as dt
import snowflake.connector as sc
import snowflake.connector.pandas_tools as pt
from datetime import datetime
from config import *

# ---------------------------------------------------------------------------------------------------#
## Snowflake Class
# ---------------------------------------------------------------------------------------------------#
class Snow:
    def __init__(self):
        self.conn = self.connect()
    
    def connect(self):
        conn = sc.connect(
        user=DB_USER,
        password=PSWD,
        account=ACCT,
        database=DB_NAME,
        warehouse=WAREHOUSE,
        role=ROLE
        )
        # declare schema for the session
        conn.cursor().execute('USE SCHEMA {};'.format(SCHEMA_NAME))
        return(conn)
    
    def execute_q(self,query):
        cur = self.conn.cursor().execute(query)
        self.conn.commit()
    
    def select_data(self,query):
        cur = self.conn.cursor().execute(query)
        data = cur.fetchall()
        return(data)

    def upload_datum(self,datum,cols,tb_name):
        qry = INSERT_TEMPLATE.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=tb_name,cols=cols,vals=datum)
        self.execute_q(qry)
    
    def tb_check(self,tb_qry):
        qry = SCHEMA_CHK.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME)
        cur = self.conn.cursor().execute(qry)
        self.conn.commit()
        cur = self.conn.cursor().execute(tb_qry)
        self.conn.commit()

    def info_table_prep(self):
        tb_qry = SERIES_INFO_TABLE_SETUP.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=SERIES_INFO_TABLE)
        self.tb_check(tb_qry)
        qry = "TRUNCATE TABLE {};".format(SERIES_INFO_TABLE)
        self.execute_q(qry)
        
    def get_all_tbs(self):
        qry = "SELECT DISTINCT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='{}';".format(SCHEMA_NAME.upper())
        cur = self.conn.cursor().execute(qry)
        df = cur.fetch_pandas_all()
        self.tables = df.applymap(lambda x: x.lower())

# ---------------------------------------------------------------------------------------------------#
## Time-series class
# ---------------------------------------------------------------------------------------------------#
class FredDatum:
    def __init__(self,name):
        self.name  = name
        # get the last update information available
        #self.exists_in_db, self.series_last_updated = self.last_datum()
    
    def get_info(self):
        fred = Fred(api_key=API_KEY)
        series_info = fred.get_series_info(self.name)
        cols = list(map(lambda x: x[0],series_info.items()))
        data = list(map(lambda x: x[1],series_info.items()))
        return(cols,data)
    # fucntion to check if table exists for this data set
    def tb_check(self,sn):
        # sn.tables.TABLE_NAME.isin([name.lower()])[0]
        qry = SERIES_TABLE_SETUP.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=self.name)
        sn.tb_check(qry)
        
    def get_last_updated(self):
        fred = Fred(api_key=API_KEY)
        series_info = fred.get_series_info(self.name)
        return(dt.datetime.fromisoformat(series_info.last_updated + ':00'))
    
    # function to check what the last update was for this data set
    def last_datum(self):
        query = LAST_UPDATE_DT_TEMPLATE.substitute(db_name=DB_NAME,
                                                   schema_name=SCHEMA_NAME,
                                                   table_name=ETL_LOG_TABLE,
                                                   series_id=self.name)
        data = sn.select_data(query)

        # if the return is null, then series has not been extracted
        if not data[0][0]:
            return([False,self.get_last_updated()])
        else:
            return([True,data[0][0]])
        
    # get the newest series data
    def api_get_data(self):
        fred = Fred(api_key=API_KEY)
        data = fred.get_series_latest_release(series_id=self.name)
        data.dropna(inplace=True)
        data = pd.DataFrame(data)
        data.reset_index(inplace=True)
        data.columns = ['DATE','VALUE']
        data['DATE'] = data['DATE'].apply(datetime.date)

        return(data)

    def get_data(self):
        self.data = self.api_get_data()
    
    def truncate_table(self,sn):
        qry = TRUNCATE_TEMPLATE.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=self.name.upper())
        sn.conn.cursor().execute(qry)
        sn.conn.commit()
    
    def update_series_db(self,sn):
        self.get_data()
        self.tb_check(sn)
        self.truncate_table(sn)
        res = self.upload_data(sn)
        return(res)

# Simple function to escape characters we need escaped
def escaper(data):
    escaped_data = []
    for in_str in data:

        escaped = str(in_str).translate(str.maketrans({"'": r"\'", '"': r'\"'}))
        escaped_data.append(escaped)

    return(escaped_data)

# Depracated
# def upload_series_info(series_info,sn):
#     tbqry = SERIES_INFO_TABLE_SETUP.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=SERIES_INFO_TABLE)
#     sn.tb_check(tbqry)
#     # clear table
#     qry = TRUNCATE_TEMPLATE.substitute(db_name=DB_NAME,schema_name=SCHEMA_NAME,table_name=SERIES_INFO_TABLE)
#     sn.conn.cursor().execute(qry)
#     sn.conn.commit()
#     # upload data
#     res = sn.upload_data(series_info,SERIES_INFO_TABLE)
#     return(res)