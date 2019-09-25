from download_stock_daily_data.get_market_data import insert_price_data
from data_base.mongodb import MongoDB_io

if __name__=='__main__':
    m = MongoDB_io()
    insert_price_data(pre_flag_=False)
    pass