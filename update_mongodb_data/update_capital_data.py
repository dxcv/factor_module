import pandas as pd
from data_base.mongodb import MongoDB_io
from download_stock_daily_data.get_capital_data_from_joinquant import get_one_day_capital,insert_capital_data,get_trade_date_list



if __name__=='__main__':
    ## 查看数据插入到哪一天
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_capital_data')
    start_date, end_date = m.get_start_end_date()
    end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')

    trade_date_list=get_trade_date_list(end_date_str)
    for date in trade_date_list:
        print(date)
        df = get_one_day_capital(date)
        insert_capital_data(df)
        pass

    pass
pass
