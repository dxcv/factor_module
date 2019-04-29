from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io


auth('15915765128','87662638qjf')
start_date='2005-01-01'

def get_trade_date_list(start_date_str=start_date):
    return get_trade_days(start_date=start_date_str, end_date=None, count=None)

def get_one_day_capital(date_str):
    q = query(valuation).filter(valuation.market_cap > 0)
    capital_df = get_fundamentals(q,date_str)
    capital_df=capital_df.loc[:,['code','day','market_cap','circulating_cap','circulating_market_cap']]
    capital_df.rename(columns={'day':'date'},inplace=True)
    return capital_df

def insert_capital_data(df):
    # 插入数据库
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_capital_data')
    m.insert_dataframe_to_mongodb(df)
    pass

if __name__=='__main__':
    trade_date_list=get_trade_date_list()
    for date in trade_date_list:
        print(date)
        capital=get_one_day_capital(date)
        insert_capital_data(capital)
        pass

    pass



pass