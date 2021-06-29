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


def read_sql(con, select_sql):
    with con.cursor() as cur:
        df = pd.read_sql(select_sql, con)
        return df

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

def make_insert_books_sql_sentence(df):
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

        sql_sentence = 'INSERT INTO books VALUES (%s,%s,%s,\
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
                    coverpicture\
                    )
        yield sql_sentence, sql_value

def make_insert_author_sql_sentence(df):
    bookdata_list = df.values.tolist()

    
    for book_data in bookdata_list:

        #ここは関数を入れる
        #authorsデータベースの取得
        #取得したデータベースの中から、
        author = book_data[7]
        author_num = numbering.author_numbering(author)
        sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
        sql_value = (author_num,author)
        yield sql_sentence, sql_value


create_table_sql = '''
                CREATE TABLE Books  (
                    ISBN CHAR(13),
                    Series_num CHAR(35),
                    GenreCode CHAR(4),
                    Genre_Target CHAR(1),
                    Genre_Category CHAR(1),
                    Genre_Content_code CHAR(2),
                    Title VARCHAR(1024),
                    Author VARCHAR(1024),
                    Description VARCHAR(1024),
                    Release_date CHAR(8),
                    CoverPicture VARCHAR(1024),
                    PRIMARY KEY (ISBN)
                );
                '''

create_author_table_sql =  '''
                CREATE TABLE authors  (
                    Author_num CHAR(10),
                    Author VARCHAR(1024),
                    PRIMARY KEY (Author_num)
                );
                '''