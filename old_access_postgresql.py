
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
