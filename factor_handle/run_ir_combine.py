#%%
from factor_handle.my_IC_test import IC_test_module
import os
from factor_handle.get_factor_backtest_necessary_data import *



factor_root_addr=r'D:\code\factor_module\factor_handle\orthogonal_factor'
factor_dict={}

for file in os.listdir(factor_root_addr):
    print(file)
    file_addr=factor_root_addr+'/'+file
    factor_name=file.strip('.csv')
    data=pd.read_csv(file_addr,index_col=0)
    factor_dict[factor_name]=data.loc[start_date:end_date]
    pass

# factor_ma_dict = {'apm':1, 'atr1m':1, 'atr3m':1, 'bcvp':5, 'bp':1, 'deltaroa':1,
#                'epttm':3, 'gt001':1, 'gt005':1, 'gt017':1, 'gt074':1, 'gt097':5, 'gt150':4, 'gt176':7,
#                'gt179':13, 'illiq':1, 'illiq_min':2, 'ivr':1, 'k':5, 'lncap':1, 'netprofitinyoy':1,
#                'ocvp':3, 'operprofitinyoy':1, 'operrevinyoy':1, 'opt_mont':4, 'return_q':1,
#                'reverse1m':1, 'reverse3m':1, 'roa':1, 'roe':1, 'roe_growth':1, 'roe_last':1,
#                'roe_stb':1, 'roe_yoy':1, 's_skew':9, 'spttm':3, 'tcv':2, 'turnover1m':1,
#                'turnover3m':1, 'turnover_ratio':1, 'up_vol_per':3, 'vr':6, 'wq001':1, 'wq004':1,
#                'wq012':5, 'wq024':2, 'wq044':7, 'wq050':1, 'wq084':1 ,'hgt_share':1 , 'hgt_share_d5':1}

factor_ma_dict = {'apm':1, 'atr1m':1, 'atr3m':1, 'bcvp':5, 'bp':1, 'deltaroa':1,
               'epttm':3, 'gt001':1, 'gt005':1, 'gt017':1, 'gt074':1, 'gt097':5, 'gt150':4, 'gt176':7,
               'gt179':13, 'illiq':1, 'illiq_min':2, 'ivr':1, 'k':5, 'lncap':1, 'netprofitinyoy':1,
               'ocvp':3, 'operprofitinyoy':1, 'operrevinyoy':1, 'opt_mont':4, 'return_q':1,
               'reverse1m':1, 'reverse3m':1, 'roa':1, 'roe':1, 'roe_growth':1, 'roe_last':1,
               'roe_stb':1,  's_skew':9, 'spttm':3, 'tcv':2, 'turnover1m':1,
               'turnover_ratio':1, 'up_vol_per':3, 'vr':6, 'wq001':1, 'wq004':1,
               'wq012':5, 'wq024':2, 'wq044':7, 'wq050':1, 'wq084':1 ,'hgt_share':1 , 'hgt_share_d5':1}

factor_rolling_mean_dict={}
for factor_name in factor_dict.keys():
    if factor_name in factor_ma_dict.keys():
        print(factor_name)
        temp=factor_dict[factor_name].rolling(
            factor_ma_dict[factor_name],min_periods = int(0.6*factor_ma_dict[factor_name])).mean()
    else:
        temp=factor_dict[factor_name]
    temp.index=pd.to_datetime(temp.index)
    factor_rolling_mean_dict[factor_name]=temp
    pass

#%%


trade_status=get_trade_status()
ipo=get_ipo_date()
stock_price=get_stock_post_adj_price()
# index_daily_data=get_zz500_price()

ic_test=IC_test_module()
ic_test.trade_status=trade_status
ic_test.stock_ipo=ipo
ic_test.stock_daily_price=stock_price
# ic_test.index_daily_data=index_daily_data

#%%
ic_df=pd.DataFrame()
for factor_name in factor_rolling_mean_dict.keys():
    print(factor_name)
    factor_data=factor_rolling_mean_dict[factor_name]
    ic_series=ic_test.IC_test(factor_data)['ic_pv_df'].ic
    ic_df[factor_name]=ic_series
    pass

# ic_df=pd.read_csv('ic_df.csv',index_col=0)
ir_df=ic_df.rolling(24*5).apply(lambda x:x.mean()/x.std())
weight_df=ir_df.div(ir_df.sum(axis=1),axis=0)
ir_weight_factor_df=pd.DataFrame()
for factor_name in factor_rolling_mean_dict.keys():
    weight_series=weight_df[factor_name]
    weight_factor=factor_rolling_mean_dict[factor_name].mul(weight_series,axis=0)
    ir_weight_factor_df=ir_weight_factor_df.add(weight_factor,fill_value=0)
    pass
ir_weight_factor_df.dropna(how='all').to_csv(r'D:\code\factor_module\factor_handle'+'/'+'ir_weight_factor.csv')
pass
