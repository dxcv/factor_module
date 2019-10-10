from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from decorate_func.decorate_function import typing_func_name

class get_trade_date_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.nothing=''
        self.start_trade_date= '1990-01-01'
        pass

    def get_trade_date_from_joinquant(self):
        print(self.nothing)
        start_trade_date=self.start_trade_date
        trade_date_list=get_trade_days(start_date=start_trade_date, end_date=None, count=None)
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


    def logging_trade_date_db(self):
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('stock_trade_date')
        pass

    @ typing_func_name
    def insert_trade_date_data(self):
        m=self.m
        logging_joinquant()
        self.logging_trade_date_db()
        m.remove_all_documents_from_mongodb()
        trade_date_info=self.get_trade_date_from_joinquant()
        m.insert_huge_dataframe_by_block_to_mongodb(trade_date_info)
        pass


# 插入数据库
if __name__=='__main__':
    a=get_trade_date_class()
    a.insert_trade_date_data()
    pass
