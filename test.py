import bookapi
from pprint import pprint
import sys

def confirm():
    rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
    bookrank = rakuten_id.request_bookrank('101266', page_count=1)
    isbn_list = rakuten_id.make_isbn_list(bookrank)
    openbd = bookapi.OpenbdApi()
    df = openbd.make_book_info_df(isbn_list)
    pprint(df)