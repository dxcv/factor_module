from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io
auth('15915765128','87662638qjf')

def get_trade_date_list(start_date_str='2010-01-01'):
    return get_trade_days(start_date=start_date_str, end_date=None, count=None)

def get_sw_industry_level1_list():
    return get_industries(name='sw_l1')

def get_one_date_industry_data(date_str,industry_code_list):
    daily_industry_series = pd.Series()
    for industry_code in industry_code_list.index[:]:
        stock_list = get_industry_stocks(industry_code, date=date_str)
        daily_industry_series = daily_industry_series.append(pd.Series(industry_code, index=stock_list))
        pass
    daily_industry_series.index=pd.MultiIndex.from_product([[date_str],daily_industry_series.index])
    daily_industry_df=daily_industry_series.to_frame().reset_index()
    daily_industry_df.columns=['date','stock','industry_category']
    return daily_industry_df
    pass

def insert_industry_stocks(df):
    # 插入数据库
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_sw_industry_code')
    m.insert_dataframe_to_mongodb(df)
    pass

if __name__=='__main__':
    trade_date_list=get_trade_date_list()
    industry_list=get_sw_industry_level1_list()
    for date in trade_date_list:
        print(date)
        one_day_data=get_one_date_industry_data(date,industry_list)
        insert_industry_stocks(one_day_data)
        pass
    pass

