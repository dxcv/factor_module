import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from factor_handle.my_single_factor_v3 import factor_test
from factor_handle.get_factor_backtest_necessary_data import *


trade_status=get_trade_status()
ipo=get_ipo_date()
stock_price=get_stock_pre_adj_price()
index_daily_data=get_zz500_price()

f=factor_test()

f.trade_status=trade_status
f.stock_ipo=ipo
f.stock_daily_price=stock_price
f.index_daily_data=index_daily_data

df=pd.read_csv(r'G:\work_space\pure_factor_deployment\neutralised_factor\apm.csv',index_col=0)
df.index=pd.to_datetime(df.index)
df= df.loc['2018-01-01':'2019-01-01']
df.columns=df.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')

f.single_factor_test(df)
