import psycopg2
import pandas as pd
from pprint import pprint

def postgres_connection(hostname, port, dbname, user, password):
    #コンテナ同士を接続しているので、hostnameはコンテナ名を入力する。localhostじゃ通じない
    #dbnameを未指定のまま、databaseをつくると、ユーザー名のデータベスがつくられるので、ユーザー名を入力している
    con = psycopg2.connect(host=hostname,\
                            port=port,\
                            dbname=dbname,\
                            user=user,\
                            password=password,\
                            )
    return con

con = postgres_connection(hostname='books_database',\
                            port='5432',\
                            dbname='ogaken5',\
                            user='ogaken5',\
                            password='books'
                            )

