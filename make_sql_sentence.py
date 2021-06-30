
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


def make_insert_sql_to_books_table(df):
    books_dict = df.to_dict(orient='records')
    for book in books_dict:

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


def make_insert_author_sql_sentence(df):
    bookdata_list = df.values.tolist()
    for book_data in bookdata_list:

        #ここは関数を入れる
        #authorsデータベースの取得
        #取得したデータベースの中から、
        author = book_data[7]
        author_num = numbering.author_numbering(author)
        sql_sentence = 'INSERT INTO authors VALUES (%s,%s)'
        sql_value = (author_num,author)
        yield sql_sentence, sql_value
