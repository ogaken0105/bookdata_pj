import bookapi

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