from jqdatasdk import *
import pandas as pd
from data_base.mongodb import MongoDB_io

## 查看数据插入到哪一天
m=MongoDB_io()
m.set_db('stock_daily_data')
m.set_collection('stock_sw_industry_category')
start_date,end_date=m.get_start_end_date()



pass
