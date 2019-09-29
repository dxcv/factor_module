import os
os.chdir(r'D:\code\factor_module\download_stock_daily_data')

from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd


def insert_zz500_data():
    m=MongoDB_io()
    m.set_db('index_daily_data')
    m.set_collection('000905_XSHG')
    m.delete_document_include_condition()


    logging_joinquant()
    df=get_price('000905.XSHG',start_date='2005-01-01',end_date='2019-09-25',fq=None,frequency='daily')
    df.dropna(inplace=True)
    ## 指数没有复权一说
    # df2=get_price('000905.XSHG',fq='pre')
    df.index.name='date'
    df.reset_index(inplace=True)
    df.date=pd.to_datetime(df.date)

    # 插入数据库
    m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass

if __name__=='__main__':
    insert_zz500_data()
    pass