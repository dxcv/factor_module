from data_base.mongodb import MongoDB_io
import pandas as pd

# db_list=['stock_capital_data',
#          'stock_ipo_date',
#          'stock_pre_price',
#          'stock_real_price',
#          'stock_sw_industry_category',
#          'stock_sw_industry_code',
#          'stock_trade_date',
#          'zz500_weight']

# db_list=['stock_capital_data',
#          'stock_post_price',
#          'stock_real_price',
#          'stock_price_adj_factor',
#          'stock_sw_industry_code',
#          'zz500_weight']


db_list=[
            # 'stock_capital_data',
            # 'stock_sw_industry_code',
            'stock_real_price_',
            # 'zz500_weight'
]

m=MongoDB_io()
m.set_db('stock_daily_data')
condition_df=pd.DataFrame()
for db in db_list:
    print(db)
    m.set_collection(db)
    document_per_date=m.get_document_in_date()
    document_per_date.name=db
    condition_df=condition_df.append(document_per_date)
    pass
# condition_df=condition_df.loc[:,'2010-01-01':]
condition_df.to_csv('update_condition.csv')


