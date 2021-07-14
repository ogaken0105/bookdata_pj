import psycopg2
import pandas as pd
from pprint import pprint

class ConnectPostgres():

    def __init__(self, host, port, dbname, user, password):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self.con = self._create_connection()

    def _create_connection(self):
        con = psycopg2.connect(host= self._host,\
                                port=self._port,\
                                dbname=self._dbname,\
                                user=self._user,\
                                password=self._password,\
                                )
        return con

class AccessTable():

    def __init__(self):
        self._con = self.connect_postgres()

    def connect_postgres(self, host, port, dbname, user, password):
        connection = ConnectPostgres(host, port, dbname, user, password)
        return connection.con

    def create_table(self, createtable_sql):
        with self._con.cursor() as cur:
            cur.execute(creqtetable_sql)
            self._con.commit()
        
    def read_table(self, select_sentence):
        with self._con.cursor() as cur:
            df = pd.read_sql(sql=select_sentence, con=self._con)
            pprint(df)

    #def insert_table(self, insert_sentence):

class SaveDf(AccessTable):

    def __init__(self, df):
        super().__init__()
        self._df = df

    def save_book_data(self):
        with self._con:
            self._con.set_client_encoding('utf-8') 
            try:
                cur = self._con.cursor()
            # テーブルを作成する SQL を準備
                instance_df = MakeSql(self._df)
                for sql_sentence, sql_value in instance_df.make_sql_to_books():
                    try:
                        cur.execute(sql_sentence,sql_value)
                    except psycopg2.IntegrityError:
                        self._con.rollback()
                    else:
                        self._con.commit()
            except Exception as e:
                print(f'Eroor{e}')

    #----出版社情報をデータベースに保存-----
    def save_publiser_data(self):
        with con:
            con.set_client_encoding('utf-8')
            bookdata_list = self._df.values.tolist()
            for book_data in bookdata_list:
                isbn = book_data[0]
                if isbn.startwith('9784') or isbn.startwith('9794'):
                    publiser_num = isbn[4:7]
                    ########出版社名を取得する必要がある

                    sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
                    sql_value = (publiser_num, publiser)
                    try:
                        cur = con.cursor()
                        try:
                            cur.execute(sql_sentence,sql_value)
                        except psycopg2.IntegrityError:
                            con.rollback()
                        else:
                            con.commit()
                    except Exception as e:
                        print(f'Eroor{e}')


    #----著者情報をデータベースに保存-----
    def save_author_data(self):
        saved_data_df = read_sql(con, 'select * from authors')
        try:
            author_count = saved_data_df.iloc[-1]['author_num']
        except:
            # ----エラー名を指定したほうがいいのでは----
            #author_numが特定の桁数になっていない
            #author_numの決め方は、出版社番号+著者番号にしたい
            author_num = 1
        else:
            author_num = author_count+1

        with con:
            con.set_client_encoding('utf-8')
            try:
                cur = con.cursor()
                bookdata_list = self._df.values.tolist()

                for book_data in bookdata_list:
                    author = book_data[7]
                    sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
                    sql_value = (author_num,author)
                    try:
                        cur.execute(sql_sentence,sql_value)
                    except psycopg2.IntegrityError:
                        con.rollback()
                    else:
                        con.commit()
                        author_num += 1
            except Exception as e:
                print(f'Eroor{e}')
















class MakeSql():
    def __init__(self, df):
        self._df = df
        self._books_dict = self._df.to_dict(orient='records')

    def make_sql_to_books(self):
        for book in self._books_dict:
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
    
    def make_sql_to_authors(self):
        for book in self._books_dict:
            author = book['author']
            author_num = numbering.author_numbering(author)
            sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
            sql_value = (author_num,author)
            yield sql_sentence, sql_value

    def make_sql_to_publishers(self):
        for book in self._books_dict:
            publisher = book['publisher']
            publisher_id_type19 = book['publisher_id_type19']
            publisher_id_type24 = book['publisher_id_type24']

            sql_sentence = 'INSERT INTO publishers VALUES (%s,%s,%s)'
            sql_value = (publisher_id_type19,publisher_id_type24,publisher)
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

