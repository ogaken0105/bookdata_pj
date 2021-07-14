import requests
import pandas as pd
import re

class Api():
    def __init__(self, endpoint_url):
        '''
        Parameters
        -----------------
        endpoint_url : str
            apiのエンドポイントURLを入力してください。
        -----------------
        '''
        self._endpoint_url = endpoint_url

    def request(self, **kwarg):
        '''
        Parameters
        -----------------
        endpoint_url : str
            apiのエンドポイントURLを入力してください。
        -----------------

        Returns
        -----------------
        response.json : jsonデータ
            APIからの返答をjson形式で返します。
        -----------------
        '''
        self._parameters = kwarg
        response = requests.get(self._endpoint_url, params=self._parameters)
        return response.json()


class RakutenBookApi(Api):

    def __init__(self, application_id):
        '''
        Parameters
        -----------------
        application_id : int
            楽天APIに登録した際に発行されるAPIキーを入力して下さい。

        '''

        super().__init__('https://app.rakuten.co.jp/services/api/IchibaItem/Ranking/20170628?format=json')
        self.__application_id = application_id

    def request_bookrank(self, genre_id, page_count):

        '''
        Parameters
        -----------------
        genre_id : int
            以下のページから、ブラウザのAPIフォーム経由でジャンルIDの確認ができます。
            ランキングを取得したいジャンルを選定し、IDを入力ください。
            書籍以外（家電など）のジャンルIDがありますが、ここでは書籍のジャンルID（ 小説・エッセイ：101266 等）を入れて下さい 
            https://webservice.rakuten.co.jp/explorer/api/IchibaGenre/Search/

        page_count : int
            取得するランキングページのページ数を指定下さい。
            １ページあたり、３０位単位で取得できます。
            １０００位まで取得可能。（３４ページ分）
        -----------------

        Returns
        -----------------

        -----------------
        '''

        for page_num in range(1,page_count+1):
            response_in_json = self.request(genreId=genre_id, applicationId=self.__application_id, page=page_num)
            yield response_in_json
    
    def process_response_items(self, response_in_json):
        for book_data in response_in_json['Items']:
            book_item = book_data['Item']
            yield book_item

    def make_isbn_list(self, gen_book_item:'generator') -> 'list':
        isbn_list =[]

        for book_item in gen_book_item:
            isbn_object = re.search('ISBN：\d+', book_item['itemCaption'])
            if isbn_object:
                isbn_num = re.sub('ISBN：', '', isbn_object.group())
            else:
                isbn_num = None

            isbn_list.append(isbn_num)

        return isbn_list

    # 使っていない
    def make_bookrank_df(self, gen_book_item):
        '''
        Parameters
        -----------------
        gen_book_item : generator
            self.request_bookran　から生成される　generator　を入力してください。
        -----------------

        Returns
        -----------------
        rakuten_book_rank_df : df
            4つの列をもつ、データフレームで返します。
            列の中身は下記になります。
            | 順位 | ISBN | タイトル | 著者 |
        -----------------
        '''
        rank_list = []
        author_list = []
        title_list = []
        isbn_list =[]
        rank = 0

        for book_item in gen_book_item:
            item_name = book_item['itemName']
            item_name = re.sub('【.+?】', '', item_name)
            item_name = re.sub('\(.+?\)', '', item_name)
            item_name = re.sub('（.+?）', '', item_name)
            author_object = re.search('\[.+?\]', item_name)

            if author_object:
                #正規表現で、[, ], 空白を削除
                author = re.sub('[\[\]\s]','',author_object.group())
            else:
                author = None

            isbn_object = re.search('ISBN：\d+', book_item['itemCaption'])
            if isbn_object:
                isbn_num = re.sub('ISBN：', '', isbn_object.group())
            else:
                isbn_num = None

            title = re.sub('\[.+?\]', '', item_name)
            title = re.sub('\s', '', title)

            rank += 1
            rank_list.append(rank)
            author_list.append(author)
            isbn_list.append(isbn_num)
            title_list.append(title)

        rakuten_book_rank_df = pd.DataFrame({'順位':rank_list, 'ISBN':isbn_list, 'タイトル':title_list, '著者':author_list})
        return rakuten_book_rank_df

#出力をdfではなく、generatorにしたい
class OpenbdApi(Api):
    def __init__(self):
        super().__init__('https://api.openbd.jp/v1/get')

    def make_book_info_df(self, isbn_list:list) -> pd.DataFrame:

        all_books_df = pd.DataFrame(
        columns=['ISBN','Series_num',\
                'GenreCode',\
                'Genre_Target',\
                'Genre_Category',\
                'Genre_Contents_code',\
                'Title',\
                'Author',\
                'author_collationkey',\
                'Publiser',\
                'publisher_id_type19',\
                'publisher_id_type24',\
                'Description',\
                'Release_date',\
                'CoverPicture'\
                ]
        )
    
        for isbn in isbn_list:
            try:
                response_in_json = self.request(isbn=isbn)

                #----取得したopenBDのAPI結果の中で、必要なものだけデータフレームに保存----
                #----全部tryにしよう！一部だけtryとかにして、取得できなかったものだけ飛ばしたい----
                isbn_num = response_in_json[0]['onix']['RecordReference']
                title = response_in_json[0]['onix']['DescriptiveDetail']['TitleDetail']['TitleElement']['TitleText']['content']
                #著者が一人ではない場合どうする？
                author = response_in_json[0]['onix']['DescriptiveDetail']['Contributor'][0]['PersonName']['content']
                author_collationkey = response_in_json[0]['onix']['DescriptiveDetail']['Contributor'][0]['PersonName']['collationkey']
                publisher = response_in_json[0]['onix']['PublishingDetail']['Imprint']['ImprintName']
                
                imprintIdentifier = response_in_json[0]['onix']['PublishingDetail']['Imprint']['ImprintIdentifier']
                for identifier in imprintIdentifier:
                    if identifier['ImprintIDType'] == '19':
                        publisher_id_type19 = identifer['IDValue']
                    elif identifier['ImprintIDType'] == '24':
                        publisher_id_type24 = identifer['IDValue']
                
                #----authorに関する情報をもっと取得する-----
                #説明文
                try:
                    Description = response_in_json[0]['onix']['CollateralDetail']['TextContent'][0]['Text']
                except:
                    Description = None
                #発売日
                try:
                    Release_date = response_in_json[0]['onix']['PublishingDetail']['PublishingDate'][0]['Date']
                except:
                    Release_date = None
                #カバージャケットの画像
                try:
                    CoverPicture = response_in_json[0]['onix']['CollateralDetail']['SupportingResource'][0]['ResourceVersion'][0]['ResourceLink']
                except:
                    CoverPicture = None
                
                #ジャンルコード（Cコード詳細）https://honno.info/category/reference/ccode_description.html
                try:
                    Genre = response_in_json[0]['onix']['DescriptiveDetail']['Subject'][0]['SubjectCode']
                    #ジャンルコード第１桁＝販売対象（Target）
                    Genre_Target = Genre[0:1]
                    #ジャンルコード第２桁＝発行形態（Category）
                    Genre_Category = Genre[1:2]
                    #ジャンルコード下２桁＝内容コード（Contents）
                    Genre_Contents_code = Genre[2:3]
                except:
                    Genre = None
                    Genre_Target = None
                    Genre_Category = None
                    Genre_Contents_code = None

                all_books_df = all_books_df.append({
                    'ISBN': isbn_num,
                    'Series_num': None, #データ取得後、データベース保存前に入力する
                    'GenreCode': Genre,
                    'Genre_Target': Genre_Target,
                    'Genre_Category': Genre_Category,
                    'Genre_Contents_code': Genre_Contents_code,
                    'Title': title,
                    'Author': author,
                    'author_collationkey': author_collationkey,
                    'Publisher': publisher,
                    'publisher_id_type19' : publisher_id_type19,
                    'publisher_id_type24' : publisher_id_type24,
                    'Description': Description,
                    'Release_date': Release_date,
                    'CoverPicture': CoverPicture,
                    }, ignore_index=True)
            except:
                pass

        return all_books_df  
