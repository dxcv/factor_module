from data_base.mongodb import MongoDB_io
from download_stock_daily_data.get_trade_day_from_joinquant import get_trade_date_from_joinquant,insert_trade_date_data
import pandas as pd



if __name__=='__main__':
    ## 查看数据插入到哪一天
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_trade_date')
    start_date, end_date = m.get_start_end_date()
    end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
    # end_date_str=end_date.strftime('%Y-%m-%d')

    trade_date=get_trade_date_from_joinquant(end_date_str)
    insert_trade_date_data(trade_date)
    pass
pass
