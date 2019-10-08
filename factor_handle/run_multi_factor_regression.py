import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from factor_handle.get_multi_factor_regression_data import *
from factor_handle.my_factor_return_cal_v2 import multi_factor_regression
import os




factor_dict={}
factor_dict['ir']=pd.read_csv(r'ir_weight_factor.csv')
# path=r'G:\work_space\pure_factor_deployment\org_factor'
# for file in os.listdir(path):
#     file_path=path.replace('\\','/')+'/'+file
#     df=pd.read_csv(file_path, index_col=0)
#     df.index=pd.to_datetime(df.index)
#     df= df.loc['2016-01-01':'2019-01-01']
#     df.columns=df.columns.map(lambda x: x[:7] + 'XSHE' if x[-1] == 'Z' else x[:7] + 'XSHG')
#     factor_name=file.strip('.csv')
#     factor_dict[factor_name]=df
#     pass

trade_status=get_trade_status()
ipo=get_ipo_date()
stock_price=get_stock_post_adj_price()
index_daily_data=get_zz500_price()
circulating_market_cap=get_capital_data()
stock_industry_code=get_stock_industry()
zz500_weight=get_zz500_weight()


MFR=multi_factor_regression()
MFR.trade_status=trade_status
MFR.ipo=ipo
MFR.stock_price=stock_price
MFR.index_daily_data=index_daily_data
MFR.circulating_market_cap=circulating_market_cap
MFR.stock_industry_code=stock_industry_code
MFR.zz500_weight=zz500_weight
result=MFR.get_all_date_factor_return(factor_dict=factor_dict)
result.to_csv('factor_return.csv')