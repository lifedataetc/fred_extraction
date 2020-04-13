import psycopg2 as pc2
from fredapi import Fred
from string import Template
from config import *

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