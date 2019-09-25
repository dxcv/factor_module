from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd




def get_one_day_capital(date_str):
    q = query(valuation).filter(valuation.market_cap > 0)
    capital_df = get_fundamentals(q,date_str)
    capital_df=capital_df.loc[:,['code','day','market_cap','circulating_cap','circulating_market_cap']]
    capital_df.rename(columns={'day':'date'},inplace=True)
    return capital_df

def logging_capital_data_db():
    global m
    # 插入数据库
    # m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_capital_data')

    pass

def insert_capital_data(initial_flag=False):
    global m
    logging_joinquant()
    logging_capital_data_db()
    if initial_flag:
        start_date=get_setting_start_date()
        trade_date_list=get_trade_date_list(start_date)
        pass
    else:
        ## 查看数据插入到哪一天
        start_date, end_date = m.get_start_end_date()
        end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
        trade_date_list = get_trade_date_list(end_date_str)
        pass

    for date in trade_date_list:
        print(date)
        capital=get_one_day_capital(date)
        m.insert_dataframe_to_mongodb(capital)
        pass
    pass



if __name__=='__main__':
    m = MongoDB_io()
    insert_capital_data()
    pass