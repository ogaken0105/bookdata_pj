### ------------------------このファイルは使用しません。------------------------
### ------------------------このファイルは使用しません。------------------------
### ------------------------このファイルは使用しません。------------------------
### ------------------------このファイルは使用しません。------------------------
### ------------------------このファイルは使用しません。------------------------

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

'''
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
'''



'''
def make_insertsql_to_books_table(df):
    books_dict = df.to_dict(orient='records')
    for book in books_dict:

        isbn = book['isbn']
        series_num = book['series_num']
        genre_code = book['genrecode']
        genre_taget = book['genre_target']
        genre_category = book['genre_category']
        genre_content_code = book['genre_content_code']
        title = book['title']
        author = book['author']
        description = book['description']
        release_date = book['release_date']
        coverpicture = book['coverpicture']
        publisher = book['publisher']
        publisher_id_type19 = book['publisher_id_type19']
        publisher_id_type24 = book['publisher_id_type24']

        sql_sentence = 'INSERT INTO books VALUES (%s,%s,%s,\
                                                %s,%s,%s,\
                                                %s,%s,%s,\
                                                %s,%s,%s,\
                                                %s,%s)'
        sql_value = (isbn,\
                    series_num,\
                    genre_code,\
                    genre_taget,\
                    genre_category,\
                    genre_content_code,\
                    title,\
                    author,\
                    description,\
                    release_date,\
                    coverpicture,\
                    publisher,\
                    publisher_id_type19,\
                    publisher_id_type24 \
                    )
        yield sql_sentence, sql_value


def make_insertsql_to_authors_table(df):
    books_dict = df.to_dict(orient='records')
    for book in books_dict:

        author = book['author']
        author_num = numbering.author_numbering(author)
        sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
        sql_value = (author_num,author)
        yield sql_sentence, sql_value

def make_insertsql_to_publishers_table(df):
    books_dict = df.to_dict(orient='records')
    for book in books_dict:

        publisher = book['publisher']
        publisher_id_type19 = book['publisher_id_type19']
        publisher_id_type24 = book['publisher_id_type24']

        sql_sentence = 'INSERT INTO publishers VALUES (%s,%s,%s)'
        sql_value = (publisher_id_type19,publisher_id_type24,publisher)
        yield sql_sentence, sql_value



def insert_data(con,df):
    with con:
        con.set_client_encoding('utf-8') 

        try:
            cur = con.cursor()

            bookdata_list = df.values.tolist()
            for book_data in bookdata_list:

                isbn = book_data[0]
                series_num = book_data[1]
                genre_code = book_data[2]
                genre_taget = book_data[3]
                genre_category = book_data[4]
                genre_content_code = book_data[5]
                title = book_data[6]
                author = book_data[7]
                description = book_data[8]
                release_date = book_data[9]
                coverpicture = book_data[10]

                #Noneが文字列で入ってしまっている
                '''
                sql_sentence = f'INSERT INTO books VALUES ({isbn},\
                                                            {series_num},\
                                                            {genre_code},\
                                                            {genre_taget},\
                                                            {genre_category},\
                                                            {genre_content_code},\
                                                            '{title}',\
                                                            '{author}',\
                                                            '{description}',\
                                                            {release_date},\
                                                            '{coverpicture}'\
                                                            );'
                '''
                
                try:
                    sql_sentence = 'INSERT INTO books VALUES (%s,%s,%s,\
                                                            %s,%s,%s,\
                                                            %s,%s,%s,\
                                                            %s,%s)'
                    cur.execute(sql_sentence,(isbn,\
                                            series_num,\
                                            genre_code,\
                                            genre_taget,\
                                            genre_category,\
                                            genre_content_code,\
                                            title,\
                                            author,\
                                            description,\
                                            release_date,\
                                            coverpicture\
                                            ))

                except psycopg2.IntegrityError:
                    con.rollback()
                else:
                    con.commit()
        except Exception as e:
            print(f'Eroor{e}')

    con.close()


def insert_author(con, df):
    with con:
        con.set_client_encoding('utf-8') 

        try:
            cur = con.cursor()

            bookdata_list = df.values.tolist()
            for book_data in bookdata_list:

                #ここは関数を入れる
                #authorsデータベースの取得
                #取得したデータベースの中から、
                author = book_data[7]
                author_num = numbering.author_numbering(author)
                
                try:
                    sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
                    cur.execute(sql_sentence,(author_num,author,))

                except psycopg2.IntegrityError:
                    con.rollback()
                else:
                    con.commit()
        except Exception as e:
            print(f'Eroor{e}')

    con.close()

#----SQLにてテーブル操作----
#----書籍情報をデータベースに保存-----


def operate_database(con, sql_sentence, sql_value=None):
    with con:
        con.set_client_encoding('utf-8') 
        try:
            cur = con.cursor()
        # テーブルを作成する SQL を準備
            try:
                cur.execute(sql_sentence,sql_value)
            except psycopg2.IntegrityError:
                con.rollback()
            else:
                con.commit()
        except Exception as e:
            print(f'Eroor{e}')
        # コミットし、変更を確定する
        con.commit()
    # 接続を閉じる
    con.close()


'''
