#%%
import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from factor_handle.my_factor_neutralise_v2 import preprocess
from factor_handle.get_preprocess_necessary_data import *


circulating_market_cap=get_capital_data()
stock_industry_code=get_stock_industry()
trade_status=get_trade_status()
ipo=get_ipo_date()
# zz500_weight=get_zz500_weight()



p=preprocess()
p.risk_factor_dict={'cap':circulating_market_cap}
p.stock_industry_df=stock_industry_code
p.trade_status_df=trade_status
p.ipo_df=ipo
# p.stock_500_component=zz500_weight

#%%
un_process_factor=pd.read_csv(r'G:\work_space\pure_factor_deployment\org_factor\apm.csv', index_col=0)
un_process_factor.index=pd.to_datetime(un_process_factor.index)
un_process_factor= un_process_factor.loc['2018-01-01':'2019-01-01']
un_process_factor.columns=un_process_factor.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')
#%%
neutralize_factor:pd.DataFrame=p.get_neutralize_z_score_data(un_process_factor)
neutralize_factor.to_csv(r'D:\code\factor_module\factor_handle\neutralize_z_score_factor\apm.csv')
