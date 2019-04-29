from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io



def get_trade_date_list(start_date_str='2010-01-01'):
    return get_trade_days(start_date=start_date_str, end_date=None, count=None)

def get_security_from_joinquant():
    return get_all_securities(types=[], date=None)
    pass

def get_pre_price(date,stock_list):
    panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False, fq='pre', count=None)
    df=panel.iloc[:,0,:]
    df.reset_index(inplace=True)
    df.rename(columns={'index':'stock'},inplace=True)
    df['date']=pd.to_datetime(date)
    return df
    pass

def insert_pre_market_data(df):
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_pre_price')
    m.insert_dataframe_to_mongodb(df)
    pass

if __name__=='__main__':
    trade_date_list=get_trade_date_list()
    stock_code_list=get_security_from_joinquant()
    for date_str in stock_code_list:
        print(date_str)
        pre_price_df=get_pre_price(date_str,stock_code_list)
        insert_pre_market_data(pre_price_df)
        pass
    pass