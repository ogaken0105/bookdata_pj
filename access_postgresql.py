#insert_dataは共通化できるところがありそう。

import psycopg2
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
        df = pd.read_sql(sql=select_sql, con=self.con)
        pprint(df)

def create_table(con, create_table_sql):
    with con:
        con.set_client_encoding('utf-8') 
        with con.cursor() as cur:
        # テーブルを作成する SQL を準備
            sql = create_table_sql
            # SQL を実行し、テーブル作成
            cur.execute(sql)
        # コミットし、変更を確定する
        con.commit()
    # 接続を閉じる
    con.close()

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