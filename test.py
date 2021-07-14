import bookapi
from  postgresql_setting import connect_postgres
#from read_database import read_sql
from pprint import pprint
import sys

#----楽天API経由で書籍ランキングを取得----
def get_isbn_from_bookrank():
    # APIインスタンスを作成
    rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
    # 書籍ランキングを取得
    bookrank = rakuten_id.request_bookrank('101266', 1)
    # 取得した書籍のISBNを取得
    isbn_list = rakuten_id.make_isbn_list(bookrank)
    return isbn_list

def get_book_info(isbn_list):
    #----OpenBDで書籍情報を取得----
    # OpenbdApiのインスタンスを作成
    openbd = bookapi.OpenbdApi()
    # 楽天ランキングで取得したISBNをインプットして、それらの書籍情報を一括取得。データベースを作成。
    df = openbd.make_book_info_df(isbn_list)
    return df

def confirm():
    rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
    bookrank = rakuten_id.request_bookrank('101266', page_count=1)
    isbn_list = rakuten_id.make_isbn_list(bookrank)
    openbd = bookapi.OpenbdApi()
    df = openbd.make_book_info_df(isbn_list)
    pprint(df)

def confirm_openbd_spec():
    openbd = bookapi.OpenbdApi()
    pprint(openbd.request(isbn='9784103362135'))

def confirm_postgesdata(con):
    df = read_sql(con, 'select * from books;')
    books = df.to_dict(orient='records')
    return books[0]['isbn']

def confirm_postgres_connection():
    ogaken5_database = connect_postgres(host='books_database',\
                                port='5432',\
                                dbname='ogaken5',\
                                user='ogaken5',\
                                password='books'
                                )
    ogaken5_database.set_table_name()
    ogaken5_database.read_table('select * from books;')



#book = next(confirm_postgesdata(con))
#pprint(confirm_postgesdata(con))
confirm_postgres_connection()


'''
#----楽天API経由で書籍ランキングを取得----
isbn_list = get_isbn_from_bookrank()
#----OpenBDで書籍情報を取得----
book_info_df = get_book_info(isbn_list)
#----書籍情報をデータベースに保存-----
save_book_data(book_info_df)
#----著者情報をデータベースに保存-----
save_author_data(book_info_df)
'''