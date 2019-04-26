from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io


auth('15915765128','87662638qjf')
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_sw_industry_code')
sw_indus=m.read_data_to_get_dataframe()

m.set_collection('stock_trade_date')
trade_day_df=m.read_data_to_get_dataframe()
trade_day_df=trade_day_df[trade_day_df.trade_date>pd.to_datetime('2010-01-01')]
trade_day_list=trade_day_df.trade_date.astype(str)

industry_code_list=sw_indus.industry_code.iloc[:34].tolist()

industry_stock_grouping=pd.DataFrame()
for date in trade_day_list:
    print(date)
    daily_industry_series=pd.Series()
    for industry_code in industry_code_list[:]:
        stock_list=get_industry_stocks(industry_code, date=date)
        daily_industry_series=daily_industry_series.append(pd.Series(industry_code,index=stock_list))
        pass
    daily_industry_series.name=pd.to_datetime(date)
    daily_industry_df=daily_industry_series.to_frame()
    daily_industry_df_stack_up=daily_industry_df.stack().reset_index()
    daily_industry_df_stack_up.columns=['stock','date','industry_category']
    m.set_collection('stock_sw_industry_category')
    m.insert_huge_dataframe_by_block_to_mongodb(daily_industry_df_stack_up)
    pass
