import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from factor_handle.my_single_factor_v3 import factor_test
from factor_handle.get_factor_backtest_necessary_data import *


trade_status=get_trade_status()
ipo=get_ipo_date()
stock_price=get_stock_post_adj_price()
index_daily_data=get_zz500_price()

f=factor_test()

f.trade_status=trade_status
f.stock_ipo=ipo
f.stock_daily_price=stock_price
f.index_daily_data=index_daily_data

# path=r'G:\work_space\pure_factor_deployment\neutralised_factor\apm.csv'
path=r'D:\code\factor_module\factor_handle\ir_weight_factor.csv'
# path=r'D:\code\factor_module\factor_handle\qjf\max_profit_hgt_pf_w24_data_bias005_feeadj.csv'

df=pd.read_csv(path,index_col=0)
df.index=pd.to_datetime(df.index)
# df= df.loc['2016-01-01':'2019-01-01']
# df.columns=df.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')

f.single_factor_test(df)
# account_dict=f.according_weight_back_test(df)
# trade_date=account_dict['nav_evening'].index
# index_data=f.index_daily_data.close
# index_data=index_data.loc[trade_date[0]:trade_date[-1]]
# index_data=index_data/index_data[0]
# compare_data=pd.DataFrame()
# compare_data['my_nav']=account_dict['nav_evening']
# compare_data['index']=index_data
# # compare_data.to_csv(r'D:\code\factor_module\factor_handle\qjf\back_test.csv')
# compare_data.to_csv(r'D:\code\factor_module\factor_handle\back_test.csv')
