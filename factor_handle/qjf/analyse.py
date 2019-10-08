from factor_handle.get_factor_backtest_necessary_data import *
stock_industry_code=get_stock_industry()
zz500_weight=get_zz500_weight()
stock_price=get_stock_post_adj_price()


#%%
import pandas as pd
data=pd.read_csv(r'D:\code\factor_module\factor_handle\qjf\max_profit_hgt_pf_w24_data_bias005_feeadj.csv',index_col=0)
data=data.loc[:'2019-07-15']
data.to_csv(r'D:\code\factor_module\factor_handle\qjf\max_profit_hgt_pf_w24_data_bias005_feeadj.ss.csv')
data.index=pd.to_datetime(data.index)
data=data.loc['2019-01-01':]
data.columns=data.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')

#%%
stock_price_=stock_price.set_index(['date','stock'])
open_price=stock_price_.open.unstack().loc[data.index]
open_price_lag_one=open_price.shift(1)
open_return_rate=open_price/open_price_lag_one-1
open_return_rate=open_return_rate.shift(-1)
# open_return_rate index:买入股票的日期，value，往后一期的收益率

#%%
for date in data.index[:1]:
    open_return_series=open_return_rate.loc[date]
    open_return_series.name='return'
    portfolio_weight_series=data.loc[date]
    portfolio_weight_series.name='my_weight'
    zz500_weight_series=zz500_weight.loc[date]
    zz500_weight_series.name='zz500_weight'
    stock_industry_code_series=stock_industry_code.loc[date]
    stock_industry_code_series.name='industry'
    concat_df=pd.concat([open_return_series,portfolio_weight_series,zz500_weight_series,stock_industry_code_series],axis=1,sort=True)
    industry_weight=concat_df[['industry','zz500_weight']].groupby('industry').sum()
    pass
