from jqdatasdk import *
from data_base.mongodb import MongoDB_io


auth('15915765128','87662638qjf')
start_date='2005-01-01'

df=get_industries(name='sw_l1')
df=df.append(get_industries(name='sw_l2'))
df=df.append(get_industries(name='sw_l3'))
df.index.name='industry_code'
df.reset_index(inplace=True)
pass

# 插入数据库
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_sw_industry_code')
m.insert_huge_dataframe_by_block_to_mongodb(df)

## 后面加上更新验证模块。

pass