from data_base.mongodb import MongoDB_io
import json

# db_list=['stock_capital_data',
#          'stock_ipo_date',
#          'stock_pre_price',
#          'stock_real_price',
#          'stock_sw_industry_category',
#          'stock_sw_industry_code',
#          'stock_trade_date',
#          'zz500_weight']

db_list=['stock_capital_data',
         'stock_pre_price',
         'stock_real_price',
         'stock_sw_industry_code',
         'zz500_weight']

dic={}
dic_len={}
last_date_dict={}
m=MongoDB_io()
m.set_db('stock_daily_data')
for db in db_list:
    print(db)
    m.set_collection(db)
    dic[db]=m.get_db_date_list()
    dic_len[db]=len(dic[db])
    last_date_dict[db] = dic[db][-1]
    pass

with open('update_condition.json','r') as f:
    json.dump(dic,f)

