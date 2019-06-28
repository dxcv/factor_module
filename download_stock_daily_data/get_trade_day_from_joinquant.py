from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io

auth('15915765128','87662638qjf')

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


def insert_trade_date_data(df):
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_trade_date')
    m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass


# 插入数据库
if __name__=='__main__':
    trade_date_info_df_outside=get_trade_date_from_joinquant()
    insert_trade_date_data(trade_date_info_df_outside)
    pass

## 后面加上更新验证模块。

pass