import access_postgresql


# -----著者データベースへアクセス
# 新規で取得した書籍情報の著者情報を抽出

rakuten_id = bookapi.RakutenBookApi('1091376391585393106')
bookrank = rakuten_id.request_bookrank('101266', page=4)
isbn_list = rakuten_id.make_isbn_list(bookrank)
openbd = bookapi.OpenbdApi()
df = openbd.make_book_info_df(isbn_list)
access_postgresql.insert_author(df)



# 既に著者データベースに登録があるか確認
# 登録がない場合、採番する

def author_numbering(author_name):
    pass
    # データベースから既存のデータを取得
    # 既存のデータにauthor_nameが含まれているか確認（authorsには、出版社番号とかも入れたほうがいい）
    # 含まれていなかったら、新規author_numを発行
    #  発行した新規author_numを採番
    #  データベースに登録