from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd


def insert_ipo_data():
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_ipo_date')
    m.delete_document_include_condition()


    logging_joinquant()
    df=get_all_securities(types=[], date=None)
    df.index.name='stock'
    df.reset_index(inplace=True)
    df.start_date=pd.to_datetime(df.start_date)
    df.end_date=pd.to_datetime(df.end_date)

    # 插入数据库
    m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass

if __name__=='__main__':
    insert_ipo_data()
    pass