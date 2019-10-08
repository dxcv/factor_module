from data_base.mongodb import MongoDB_io
import pandas as pd

m=MongoDB_io()

def get_stock_daily_post_price(start_date=None):
    m.set_db('stock_daily_data')
    m.set_collection('stock_post_price')
    df=m.read_data_to_get_field_include_condition(['stock','close','date'],start_date=start_date)
    return df
    pass

def get_stock_daily_real_price(start_date=None):
    m.set_db('stock_daily_data')
    m.set_collection('stock_real_price')
    df=m.read_data_to_get_field_include_condition(['stock','close','date'],start_date=start_date)
    return df
    pass

def merge_real_post_price(start_date=None):
    if start_date is None:
        post_price=get_stock_daily_post_price()
        real_price=get_stock_daily_real_price()
    else:
        post_price=get_stock_daily_post_price(start_date)
        real_price=get_stock_daily_real_price(start_date)
    merge_df=pd.merge(post_price,real_price,how='outer',on=['stock','date'],suffixes=('_post','_real'))
    return merge_df

def calculate_adj_factor(start_date=None):
    merge_price=merge_real_post_price(start_date)
    merge_price['adj_factor']=merge_price.close_post/merge_price.close_real
    merge_price_=merge_price.set_index(['date','stock']).dropna()
    adj_factor_df=merge_price_.adj_factor.reset_index()
    return adj_factor_df
    pass

def insert_adj_factor(initial_flag=True):
    if initial_flag:
        adj_factor=calculate_adj_factor()
        m.set_collection('stock_price_adj_factor')
        date_list=adj_factor.date.drop_duplicates()
        for date in date_list:
            print(date)
            m.insert_dataframe_to_mongodb(adj_factor[adj_factor.date==date])
            pass
    else:
        m.set_collection('stock_price_adj_factor')
        _,end_date=m.get_start_end_date()
        end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
        adj_factor=calculate_adj_factor(end_date_str)
        m.insert_huge_dataframe_by_block_to_mongodb(adj_factor)





insert_adj_factor(True)
