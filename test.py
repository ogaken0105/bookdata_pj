import bookapi
from  access_postgresql import read_sql, con
from pprint import pprint
import sys

def confirm():
    rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
    bookrank = rakuten_id.request_bookrank('101266', page_count=1)
    isbn_list = rakuten_id.make_isbn_list(bookrank)
    openbd = bookapi.OpenbdApi()
    df = openbd.make_book_info_df(isbn_list)
    pprint(df)

def confirm_openbd_spec():
    openbd = bookapi.OpenbdApi()
    pprint(openbd.request(isbn='9784103330639'))

def confirm_postgesdata(con):
    df = read_sql(con, 'select * from books;')
    books = df.to_dict(orient='records')
    return books[0]['isbn']

#book = next(confirm_postgesdata(con))
pprint(confirm_postgesdata(con))




#----楽天API経由で書籍ランキングを取得----
isbn_list = get_isbn_from_bookrank()
#----OpenBDで書籍情報を取得----
book_info_df = get_book_info(isbn_list)
#----書籍情報をデータベースに保存-----
save_book_data(book_info_df)
#----著者情報をデータベースに保存-----
save_author_data(book_info_df)