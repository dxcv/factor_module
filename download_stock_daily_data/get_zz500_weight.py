from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd



def get_zz500_weight(date_str):
    weight_df = get_index_weights('000905.XSHG', date=date_str).reset_index()
    weight_df.date=pd.to_datetime(weight_df.date)
    return weight_df
    pass

def logging_zz500_weight():
    # 插入数据库
    global m
    # m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('zz500_weight')
    # m.insert_dataframe_to_mongodb(weight_df)
    pass


def insert_zz500_weight(initial_flag=False):
    global m
    logging_joinquant()
    logging_zz500_weight()
    if initial_flag:
        start_date=get_setting_start_date()
        trade_date_list=get_trade_date_list(start_date)
    else:
        start_date, end_date = m.get_start_end_date()
        end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
        trade_date_list = get_trade_date_list(end_date_str)
        pass

    for date in trade_date_list:
        print(date)
        df=get_zz500_weight(date)
        m.insert_huge_dataframe_by_block_to_mongodb(df)
    pass

if __name__=='__main__':
    ## 查看数据插入到哪一天
    m = MongoDB_io()
    insert_zz500_weight()
    pass

pass

pass