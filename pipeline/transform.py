import pandas as pd

def transform_etl_job_4(_result):
    _key_table = _result[2]
    _f_merge = _result[0]
    _s_merge = _result[1]

    _merge_df = _key_table.merge(_f_merge, on= 'actor_id', how = 'inner', suffixes = ('_film_actor', '_actor'))
    _merge_df = _merge_df.merge(_s_merge, on= 'film_id', how = 'inner')

    if len(_key_table) == len(_merge_df):
        return _merge_df
    else:
        raise Exception('데이터프레임이 잘못 병합된것 같아요!!!') 
    
def transform_etl_job_6(_result):
    """
    Input 
        1) _result (list(dict))
    Output
        1) _transform_result (pandas.core.frame.DataFrame)
    Note
        1) 입력으로 받은 list(dict()) 형태의 데이터를 pandas DataFrame 으로 변환해 주기 위한 함수 
    """

    _transform_result = pd.DataFrame(_result)

    return _transform_result

def transform_hannahoh(_transform_df):
    """
    Input 
    1) _transform_df (pandas.core.frame.DataFrame)
    Output
    1) _result_df (pandas.core.frame.DataFrame)
    Note
    1) 입력받은 데이터프레임의 상품번호별 판매지수평균을 구한 후 하나의 데이터프레임으로 출력하기 위한 함수
    2) 입력받은 상품번호의 수와 적재된 상품번호의 수가 같지않으면 종료됩니다.
    """
    _transform_df['년월'] = pd.to_datetime(_transform_df['크롤링 날짜']).dt.strftime('%Y%m')
    _one_df = _transform_df[['년월', '상품번호', '상품명', '저자', '브랜드', '학제', '과목']].drop_duplicates(['상품번호'])
    avg_df = pd.DataFrame(_transform_df.groupby('상품번호')['판매지수'].mean()).rename(columns = {'판매지수':'판매지수평균'})
    _result_df = _one_df.merge(avg_df, how='inner', on='상품번호')
    _result_df.columns = ['yyyymm', 'code', 'name', 'author', 'book_group', 'level', 'subject', 'sales_qty_avg']

    if len(_result_df) == len(_one_df):
        return _result_df
    else:
        print(f'<<<<< KILL BATCH : 상품번호의 수가 같지 않습니다!!! >>>>>')
        print(f'원래 데이터프레임 길이: {len(_one_df)}, 적재된 데이터프레임 길이: {len(_result_df)}')
        raise Exception('데이터프레임이 잘못 병합된것 같아요!!!') 