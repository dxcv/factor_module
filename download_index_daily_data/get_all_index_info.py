import os
os.chdir(r'D:\code\factor_module\download_stock_daily_data')

from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd


def insert_index_data():
    m=MongoDB_io()
    m.set_db('index_daily_data')
    m.set_collection('index_info')
    m.delete_document_include_condition()


    logging_joinquant()
    df=get_all_securities(types='index', date=None)
    df.index.name='index'
    df.reset_index(inplace=True)
    df.start_date=pd.to_datetime(df.start_date)
    df.end_date=pd.to_datetime(df.end_date)

    # 插入数据库
    m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass

if __name__=='__main__':
    insert_index_data()
    pass