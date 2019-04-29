from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io

def insert_ipo_data():
    auth('15915765128','87662638qjf')
    df=get_all_securities(types=[], date=None)
    df.index.name='stock'
    df.reset_index(inplace=True)
    df.start_date=pd.to_datetime(df.start_date)
    df.end_date=pd.to_datetime(df.end_date)

    # 插入数据库
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_ipo_date')
    m.insert_huge_dataframe_by_block_to_mongodb(df)

    pass
## 后面加上更新验证模块。

pass