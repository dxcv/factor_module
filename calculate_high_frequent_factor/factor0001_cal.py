from calculate_high_frequent_factor.factor0002 import calculate_stat
from data_base.mongodb import MongoDB_io
import pandas as pd

#%% 获得交易日
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_trade_date')
trade_date_info=m.read_data_to_get_dataframe()
trade_date=trade_date_info[(trade_date_info.date>pd.to_datetime('2018-01-01'))&(
        trade_date_info.date<pd.to_datetime('2019-01-01'))]['date'].astype(str).tolist()

#%% 获得指数数据
m.set_MongoClient()
m.set_db('index_min_info')
m.set_collection('000001')
index_data=m.read_data_to_get_dataframe()

#%% 获得股票代码列表
m.set_db('stock_min_info')
stock_code=m.list_collection_names()
stock_code.sort()



#%%
for stock in stock_code[:1]:
    print(stock)
    m.set_collection(stock)
    stock_min_data=m.read_data_to_get_dataframe()
    factor_data=calculate_stat(stock_min_data,trade_date,index_min_data=index_data)
    pass
