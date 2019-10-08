#%%
import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from factor_handle.my_preprocess import preprocess
from factor_handle.get_preprocess_necessary_data import *
import os


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
factor_path=r'D:\code\qjf\work_space\pure_factor_deployment\original_factor'
original_factor_dict={}
for file in os.listdir(factor_path):
    print(file)
    file_path=factor_path+'/'+file
    factor_name=file.strip('.csv')
    un_process_factor=pd.read_csv(file_path, index_col=0)
    un_process_factor.index=pd.to_datetime(un_process_factor.index)
    un_process_factor= un_process_factor.loc[start_date:end_date]
    un_process_factor.columns=un_process_factor.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')
    original_factor_dict[factor_name]=un_process_factor
#%%
for factor_name in original_factor_dict.keys():
    print(factor_name)
    neutralize_factor:pd.DataFrame=p.get_neutralize_z_score_data(original_factor_dict[factor_name])
    neutralize_factor.to_csv(r'D:\code\factor_module\factor_handle\neutralize_z_score_factor'+'/'+factor_name+'.csv')
