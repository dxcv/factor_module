from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd

def get_security_from_joinquant():
    df = get_all_securities(types=[], date=None)
    return df.index.tolist()
    pass

def get_stock_daily_price(date, stock_list,pre_flag=None):
    if pre_flag=='pre':
        panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                          fq='pre', count=None)
    elif pre_flag=='post':
        panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                          fq='post', count=None)
    else:
        panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                          fq=None, count=None)
        pass
    df=panel.iloc[:,0,:]
    df.reset_index(inplace=True)
    df.rename(columns={'index':'stock'},inplace=True)
    df['date']=pd.to_datetime(date)
    return df
    pass

def logging_market_data_db(mongo_handle,pre_flag=None):
    m=mongo_handle
    m.set_db('stock_daily_data')
    if pre_flag=='pre':
        m.set_collection('stock_pre_price')
    elif pre_flag=='post':
        m.set_collection('stock_post_price')
    else:
        m.set_collection('stock_real_price')
    # m.insert_dataframe_to_mongodb(df)
    pass

def insert_price_data(initial_flag=False,pre_flag_ = None):
    m = MongoDB_io()
    logging_joinquant()
    logging_market_data_db(m,pre_flag=pre_flag_)
    stock_code_list=get_security_from_joinquant()
    if initial_flag:
        start_date=get_setting_start_date()
        trade_date_list=get_trade_date_list(start_date)
    else:
        start_date, end_date = m.get_start_end_date()
        end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
        trade_date_list = get_trade_date_list(end_date_str)

    for date_str in trade_date_list:
        print(date_str)
        price_df=get_stock_daily_price(date_str, stock_code_list,pre_flag=pre_flag_)
        m.insert_huge_dataframe_by_block_to_mongodb(price_df)
        pass
    pass

def insert_real_price_data(initial_flag=False):
    insert_price_data(initial_flag=initial_flag,pre_flag_ = None)
    pass

if __name__=='__main__':
    # insert_price_data()
    insert_real_price_data()
    pass