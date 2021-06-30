from pprint import pprint
import bookapi
import access_postgresql
import test 
import sys

def main():
    rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
    bookrank = rakuten_id.request_bookrank('101266', page=4)
    isbn_list = rakuten_id.make_isbn_list(bookrank)
    openbd = bookapi.OpenbdApi()
    df = openbd.make_book_info_df(isbn_list)
    return df

'''
def access_insert_sql(df):
    #コンテナ同士を接続しているので、hostnameはコンテナ名を入力する。localhostじゃ通じない
    #dbnameを未指定のまま、databaseをつくると、ユーザー名のデータベスがつくられるので、ユーザー名を入力している
    postgres = sql.HandlePostgreSql(hostname='books_database',\
                                    port='5432',\
                                    dbname='ogaken5',\
                                    username='ogaken5',\
                                    password='books',\
                                    table_name='books',\
                                    )


    bookdata_list = df.values.tolist()
    for book_data in bookdata_list:

        isbn = book_data[0]
        series_num = book_data[1]
        genre_code = book_data[2]
        genre_taget = book_data[3]
        gente_category = book_data[4]
        genre_content_code = book_data[5]
        title = book_data[6]
        author = book_data[7]
        description = book_data[8]
        release_date = book_data[9]
        coverpicture = book_data[10]

        #Noneが文字列で入ってしまっている
        sql_sentence = f"INSERT INTO books VALUES ('{isbn}',\
                                                    '{series_num}',\
                                                    '{genre_code}',\
                                                    '{genre_taget}',\
                                                    '{gente_category}',\
                                                    '{genre_content_code}',\
                                                    '{title}',\
                                                    '{author}',\
                                                    '{description}',\
                                                    '{release_date}',\
                                                    '{coverpicture}'\
                                                    );" 
        postgres.insert_table(sql_sentence,no_commit=True)
    postgres.con.commit()
'''

'''
if __name__ == '__main__':
    df = main()
    #-----openbdの取得負荷を減らすために-----
    #insertする前に、既に保存されているisbnリストを取得する
    #既に取得しているisbnがあったら、insertしない
    
    access_postgresql.insert_data(df)
    #ここから実装する
    #-----著者番号を付与する-----
    #新規取得したデータの中で、著者を抽出。それぞれの著者について、著者番号をデータベースより取得
    #同一著者が存在したら、それを著者番号として、保存
    #同一著者が存在しなかったら、著者番号を発行して付与
    #
    #-----シリーズ番号の付与-----
    #保存したら、シリーズ番号を付与するために、シリーズ番号が空のリストを取得する
    #空のリストの中から、著者ごとにSQLからデータ取得
    #著者データの中から、シリーズ番号を取得
    #同一シリーズが存在しなかったら、新たにシリーズ番号を発行する
    #データ保存
    
    #sql_insert.test_create_table()
'''

if __name__ == '__main__':
    #test.confirm()
    access_postgresql.create_author_table()