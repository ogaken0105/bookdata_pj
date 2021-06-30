from access_postgresql import con, read_sql, operate_database, create_author_table_sql,make_insert_books_sql_sentence
from pprint import pprint
import bookapi
import psycopg2

#----SQLにてテーブル操作----
#----書籍情報をデータベースに保存-----
def save_book_data(df):
    with con:
        con.set_client_encoding('utf-8') 
        try:
            cur = con.cursor()
        # テーブルを作成する SQL を準備
            for sql_sentence, sql_value in make_insert_books_sql_sentence(df):
                try:
                    cur.execute(sql_sentence,sql_value)
                except psycopg2.IntegrityError:
                    con.rollback()
                else:
                    con.commit()
        except Exception as e:
            print(f'Eroor{e}')

#----出版社情報をデータベースに保存-----
def save_publiser_data(df):
    with con:
        con.set_client_encoding('utf-8')
        bookdata_list = df.values.tolist()
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
def save_author_data(df):
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
            bookdata_list = df.values.tolist()

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


'''
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
