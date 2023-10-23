from ast import literal_eval

from settings import DB_SETTINGS

from db.connector import DBConnector
from db.queries_rdb import queries_rdb
from db.queries_ddb import queries_ddb

from pipeline import extract, transform, load

import pandas as pd 


def hannahoh(_batch_month):

    from datetime import datetime
    
    print('<< Start hannahoh >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_collection_list = ['math_book']
    _target_table_list = [f'{_batch_month}math_book_analytics_hannahoh']

    for _idx, _coll in enumerate(_source_collection_list):
        print(f"#####-----  Start Extract : collection_name == '{_coll}'  -----#####")
        _yyyymm = datetime.strptime(_batch_month, '%Y%m').strftime('%Y-%m')
        _raw_query = (queries_ddb['read']['math_book'].split('}}')[0]) + '}' + f', "크롤링 날짜": {{"$regex": "^{_yyyymm}"}}' + '}\n'
        _read_query = literal_eval(_raw_query.strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(data)}'  -----#####")

        _transform_df = transform.transform_etl_job_6(data)

        _tb_name_1 = _target_table_list[_idx]
        print( f"#####-----  Start Load : table_name == '{_tb_name_1}'  -----#####")
        load.rdb_pandas_loader(_target_db_conn, _tb_name_1, _transform_df)
        print(f"#####-----  End Load  -----#####")

    _tb_name_2 = 'online_data'
    sales_qty = transform.transform_hannahoh(_transform_df)
    print( f"#####-----  Start Load : table_name == '{_tb_name_2}'  -----#####")
    load.rdb_pandas_loader(_target_db_conn, _tb_name_2, sales_qty)
    print(f"#####-----  End Load  -----#####")

    print('<< End hannahoh >>')


def etl_job_1(_batch_month):
    print('<< Start etl_job_1 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film']
    _target_table_list = ['actor_back_v1', 'film_back_v1']

    for _idx, _tb in enumerate(_source_table_list):

        print(f"#####-----  Start Extract : table_name == '{_tb}'  -----#####")
        _read_query = queries_rdb['read'][_tb]
        data = extract.rdb_cursor_extractor(_source_db_conn, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(data)}'  -----#####")

        _tb_back = _target_table_list[_idx]
        print(f"#####-----  Start Load : table_name == '{_tb_back}'  -----#####")
        _create_query = queries_rdb['create'][_tb_back]
        load.rdb_cursor_loader(_target_db_conn, _create_query, data)
        print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_1 >>')


def etl_job_2(_batch_month):
    print('<< Start etl_job_2 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film', 'film_actor']
    _target_table_list = ['actor_back_v1', 'film_back_v1', 'film_actor_back_v1']

    for _idx, _tb in enumerate(_source_table_list):

        print(f"#####-----  Start Extract : table_name == '{_tb}'  -----#####")
        _read_query = queries_rdb['read'][_tb]
        pdf = extract.rdb_pandas_extractor(_source_db_conn, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(pdf)}'  -----#####")

        _tb_back = _target_table_list[_idx]
        print( f"#####-----  Start Load : table_name == '{_tb_back}'  -----#####")
        load.rdb_pandas_loader(_target_db_conn, _tb_back, pdf)
        print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_2 >>')


def etl_job_3(_batch_month):
    print('<< Start etl_job_3 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor_yyyymm', 'film_yyyymm']
    _target_table_list = ['actor_back_v2', 'film_back_v2']

    for _idx, _tb in enumerate(_source_table_list):

        print(f"#####-----  Start Extract : table_name == '{_tb}'  -----#####")
        _read_query = queries_rdb['read'][_tb].format(*[_batch_month])
        data = extract.rdb_cursor_extractor(_source_db_conn, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(data)}'  -----#####")

        _tb_back = _target_table_list[_idx]
        print(f"#####-----  Start Load : table_name == '{_tb_back}'  -----#####")
        _create_query = queries_rdb['create'][_tb_back]
        load.rdb_cursor_loader(_target_db_conn, _create_query, data)
        print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_3 >>')

def etl_job_4(_batch_month):
    print('<< Start etl_job_4 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film', 'film_actor']
    _target_table_list = []

    _pdfs = []
    for _idx, _tb in enumerate(_source_table_list):

        print(f"#####-----  Start Extract : table_name == '{_tb}'  -----#####")
        _read_query = queries_rdb['read'][_tb]
        pdf = extract.rdb_pandas_extractor(_source_db_conn, _read_query)
        _pdfs.append(pdf)
        print(f"#####-----  End Extract : row_cnt == '{len(pdf)}'  -----#####")
        
    print(f"#####-----  Start Transform  -----#####")
    _transform_df = transform.transform_etl_job_4(_pdfs)
    print(f"#####-----  End Transform  -----#####")

    _tb_name = 'join_table_actor_and_film'
    print( f"#####-----  Start Load : table_name == '{_tb_name}'  -----#####")
    load.rdb_pandas_loader(_target_db_conn, _tb_name, _transform_df)
    print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_4 >>')


def etl_job_5(_batch_month):
    print('<< Start etl_job_5 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_ddb'])

    _source_collection_list = ['book_code']
    _target_collection_list = ['book_code_back_v2']

    for _idx, _coll in enumerate(_source_collection_list):

        print(f"#####-----  Start Extract : collection_name == '{_coll}'  -----#####")
        _read_query = literal_eval(queries_ddb['read'][_coll].strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(data)}'  -----#####")

        _coll_back = _target_collection_list[_idx]
        print(f"#####-----  Start Load : collection_name == '{_coll_back}'  -----#####")
        load.ddb_cursor_loader(_target_db_conn, _coll_back, data)
        print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_5 >>')

def etl_job_6(_batch_month):
    print('<< Start etl_job_6 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_collection_list = ['book_code']
    _target_table_list = ['book_code_basic_back']

    for _idx, _coll in enumerate(_source_collection_list):
        print(f"#####-----  Start Extract : collection_name == '{_coll}'  -----#####")
        _read_query = literal_eval(queries_ddb['read'][_coll].strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"#####-----  End Extract : row_cnt == '{len(data)}'  -----#####")

        _transform_df = transform.transform_etl_job_6(data)

        _tb_name = _target_table_list[_idx]
        print( f"#####-----  Start Load : table_name == '{_tb_name}'  -----#####")
        load.rdb_pandas_loader(_target_db_conn, _tb_name, _transform_df)
        print(f"#####-----  End Load  -----#####")

    print('<< End etl_job_6 >>')
