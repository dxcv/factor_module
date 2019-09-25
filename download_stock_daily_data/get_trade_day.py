from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd


def get_trade_date_from_joinquant(start_date='2005-01-01'):
    trade_date_list=get_trade_days(start_date=start_date, end_date=None, count=None)
    trade_date_info_df=pd.DataFrame()
    trade_date_info_df['date']=trade_date_list
    trade_date_info_df['weekday']=trade_date_info_df['date'].apply(lambda x:x.weekday())+1.0
    trade_date_info_df['trade_month']=trade_date_info_df['date'].apply(lambda x:str(x)[:7])
    trade_date_info_df['week_ordinal_in_year']=trade_date_info_df['date'].apply(lambda x:x.strftime('%W'))

    def get_ordinal_of_date(x):
        x['ordinal_in_month']=range(x.shape[0])
        x['ordinal_in_month']=x['ordinal_in_month']+1.0
        return x
        pass

    trade_date_info_df=trade_date_info_df.groupby('trade_month').apply(get_ordinal_of_date)
    trade_date_info_df['date']=pd.to_datetime(trade_date_info_df['date'])
    return trade_date_info_df
    pass


def logging_trade_date_db():
    global m
    # m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_trade_date')
    # m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass

def insert_trade_date_data():
    global m
    logging_joinquant()
    logging_trade_date_db()
    m.delete_document_include_condition()
    trade_date_info=get_trade_date_from_joinquant()
    m.insert_huge_dataframe_by_block_to_mongodb(trade_date_info)
    pass


# 插入数据库
if __name__=='__main__':
    m=MongoDB_io()
    insert_trade_date_data()
    pass
pass