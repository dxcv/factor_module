from download_stock_daily_data.get_pre_market_data import *
import pandas as pd
from data_base.mongodb import MongoDB_io

if __name__=='__main__':
    ## 查看数据插入到哪一天
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_pre_price')
    start_date, end_date = m.get_start_end_date()
    end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')

    trade_date_list=get_trade_date_list(end_date_str)
    stock_code_list=get_security_from_joinquant()
    for date_str in trade_date_list:
        print(date_str)
        pre_price_df=get_pre_price(date_str,stock_code_list)
        insert_pre_market_data(pre_price_df)
        pass

pass
