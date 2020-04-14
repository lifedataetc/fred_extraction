import psycopg2 as pc2
from fredapi import Fred
from string import Template
from config import *

# function to create data refresh queries
def yc_refresh():
    # date extraction
    cte_template = Template("""$series AS(SELECT DISTINCT ON (date) date,value FROM fin_ts.$table_name)""")

    date_select = Template("""(SELECT DISTINCT date FROM $cte_name)\n""")

    # build out the yield curve data set
    main_q_template = Template("""
    SELECT
        AD.*,
    $select_block
    INTO fin_ts.yield_curve
    FROM fin_ts.all_dates AS AD
    $join_block;
    """)

    join_q_template = Template("""
    LEFT OUTER JOIN $table_name AS $series
        ON AD.date = $series.date""")

    select_q_template = Template("""    $series.value AS ${series}_rate,""")

    # build out all the cte statements
    all_cte_temp = []
    all_dates_temp = []
    for each in SERIES:
        all_cte_temp.append(cte_template.substitute(series = each, table_name = each.lower()))
        all_dates_temp.append(date_select.substitute(cte_name=each))

    # create cte block
    cte_block = 'WITH '+',\n'.join(all_cte_temp) + ',\n'

    # get all dates
    all_dates = 'UNION\n'.join(all_dates_temp)

    # build out an indexed table of all dates to build yield curves later
    all_date_block = 'DROP TABLE IF EXISTS fin_ts.all_dates;\n'
    all_date_block = all_date_block + cte_block + 'all_dates_temp AS(' + all_dates + ')'
    all_date_block = all_date_block + 'SELECT DISTINCT date INTO fin_ts.all_dates FROM all_dates_temp ORDER BY date DESC;\n'
    all_date_block = all_date_block + 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA fin_ts TO fred;\nCREATE INDEX idx_fred_dates ON fin_ts.all_dates(date);'

    joins_temp = []
    select_temp = []
    for each in SERIES:
        joins_temp.append(join_q_template.substitute(table_name=each.lower(),series=each))
        select_temp.append(select_q_template.substitute(series=each))

    join_block = ''.join(joins_temp)
    select_block = '\n'.join(select_temp)[:-1]

    full_query = 'DROP TABLE IF EXISTS fin_ts.yield_curve;\n'
    full_query = full_query + cte_block[:-2] + main_q_template.substitute(select_block=select_block,join_block=join_block)
    full_query = full_query + 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA fin_ts TO fred;\nCREATE INDEX idx_yc_dates ON fin_ts.yield_curve(date);'

    return([all_date_block,full_query])

def connect_and_execute(query):
    try:
        # connect to the system
        conn = pc2.connect(CONNECTION_STRING)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
    # some basic error handling
    except pc2.Error as err:
        print(str(err))
        conn.rollback()
        cur.close()
        conn.close()
    except Exception as err:
        print(str(err))
        conn.rollback()
        cur.close()
        conn.close()

# Assign API key
class fred_datum:
    def __init__(self,name):
        self.name  = name
        self.schema_name = SCHEMA_NAME
        self.tb_exists = self.db_check()
        #self.last_datum = self.last_datum()
        self.new_data = self.get_all_data()

    # fucntion to check if table exists for this data set
    def db_check(self):
        conn = pc2.connect(CONNECTION_STRING)
        cur = conn.cursor()
        query = table_check_template.substitute(schema_name=self.schema_name.lower(),
                                                table_name=self.name.lower())
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        conn.close()

        return(data[0][0])

    # not used at the moment
    # function to check what the last update was for this data set
    def last_datum(self):
        if self.tb_exists:
            conn = pc2.connect(CONNECTION_STRING)
            cur = conn.cursor()
            query = last_date_template.substitute(schema_name=self.schema_name.lower(),
                                                  table_name=self.name.lower())
            cur.execute(query)
            data = cur.fetchall()
            cur.close()
            conn.close()

            return(data[0][0])
        else:
            return(0)

    # get newest data for the current data set
    def get_all_data(self):
        fred = Fred(api_key=API_KEY)
        data = fred.get_series_all_releases(series_id=self.name)
        data.dropna(inplace=True)
        data['realtime_start'] = data.apply(lambda x: str(x.realtime_start.date()),axis=1)
        data['date'] = data.apply(lambda x: str(x.date.date()),axis=1)

        return(data)

    def upload_data(self):
        insert_q = insert_template.substitute(schema_name=self.schema_name.lower(),
                                              table_name=self.name.lower())

        drop_q = drop_template.substitute(schema_name=self.schema_name.lower(),
                                                  table_name=self.name.lower())

        create_q = create_template.substitute(schema_name=self.schema_name.lower(),
                                              table_name=self.name.lower())

        try:
            # connect to the system
            conn = pc2.connect(CONNECTION_STRING)
            cur = conn.cursor()
            cur.execute(drop_q)
            cur.execute(create_q)
            cur.executemany(insert_q, list(self.new_data.to_records(index=False)))
            conn.commit()
            cur.close()
            conn.close()
            return(True)

        # some basic error handling
        except pc2.Error as err:
            return(str(err))
            conn.rollback()
            cur.close()
            conn.close()
        except Exception as err:
            return(str(err))
            conn.rollback()
            cur.close()
            conn.close()

    def process_data(self):
        flag = self.upload_data()
        if flag:
            out_log = 'None'
        else:
            out_log = flag

        return(out_log)