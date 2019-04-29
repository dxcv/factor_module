from jqdatasdk import *
from data_base.mongodb import MongoDB_io
auth('15915765128','87662638qjf')
import pandas as pd

def get_trade_date_list(start_date_str='2010-01-01'):
    return get_trade_days(start_date=start_date_str, end_date=None, count=None)

def get_zz500_weight(date_str):
    weight_df = get_index_weights('000905.XSHG', date=date_str).reset_index()
    weight_df.date=pd.to_datetime(weight_df.date)
    return weight_df
    pass

def insert_zz500_weight(weight_df):
    # 插入数据库
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('zz500_weight')
    m.insert_dataframe_to_mongodb(weight_df)
    pass


if __name__=='__main__':
    trade_date_list=get_trade_date_list()
    for date in trade_date_list:
        df=get_zz500_weight(date)
        insert_zz500_weight(df)
    pass


pass