###このファイルは使用しません。

import os
import psycopg2
from pprint import pprint
import pandas as pd

class HandlePostgreSql():

    def __init__(self, hostname, port, dbname, username, table_name, password=None):

        self.hostname = hostname
        self.port = port
        self.dbname = dbname
        self.username = username
        self.password = password

        self.table_name = table_name

        self.connection_info = f'host={self.hostname} \
                                port={self.port}\
                                dbname={self.dbname} \
                                user={self.username}\
                                password={self.password}'

        self.con = psycopg2.connect(self.connection_info)


    def create_table(self, createtable_sql):
        with self.con.cursor() as cur:
            cur.execute(creqtetable_sql)
            self.con.commit()
        
    def read_table(self, select_sql):
        with self.con.cursor() as cur:
            df = pd.read_sql(sql=select_sql, con=self.con)
            pprint(df)
    '''
    def insert_table(self, insert_sql, no_commit=False):
        with self.con as con:
            with con.cursor() as cur:
                cur.execute(insert_sql)
                if no_commit:
                    pass
                else:
                    self.con.commit()
    '''