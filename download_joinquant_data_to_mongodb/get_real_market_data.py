from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io


auth('15915765128','87662638qjf')
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_sw_industry_code')
sw_indus=m.read_data_to_get_dataframe()

start_date='2010-01-01'
trade_date_list=get_trade_days(start_date=start_date, end_date=None, count=None)
group_day_num=1000
group_num=1

trade_date_list=get_trade_days(start_date=start_date, end_date=None, count=None)
weight_df=pd.DataFrame()

m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_ipo_date')
ipo_df=m.read_data_to_get_dataframe()
stock_list=ipo_df.stock.tolist()

for date in trade_date_list:
    print(date)
    panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False, fq='none', count=None)
    df=panel.iloc[:,0,:]
    df.reset_index(inplace=True)
    df.rename(columns={'index':'stock'},inplace=True)
    df['date']=pd.to_datetime(date)
    m.set_collection('stock_real_price')
    m.insert_dataframe_to_mongodb(df)
    pass