from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io


auth('15915765128','87662638qjf')

start_date='2005-01-01'
group_day_num=1000
group_num=4

trade_date_list=get_trade_days(start_date=start_date, end_date=None, count=None)


weight_df=pd.DataFrame()
for date in trade_date_list[(group_num-1)*group_day_num:group_num*group_day_num]:
    print(date)
    q = query(valuation).filter(valuation.market_cap > 0)
    df = get_fundamentals(q,date)
    df=df.loc[:,['code','day','market_cap','circulating_cap','circulating_market_cap']]
    df.rename(columns={'day':'date'},inplace=True)
    # 打印出总市值
    weight_df=weight_df.append(df)
    pass

## df 格式修改
weight_df.date=pd.to_datetime(weight_df.date)
if 'index' in weight_df.columns:
    weight_df.drop('index',axis=1,inplace=True)
    pass
print('transfer done')

# 插入数据库
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_capital_data')
m.insert_huge_dataframe_by_block_to_mongodb(weight_df)

pass