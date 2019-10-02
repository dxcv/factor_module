import numpy as np
import pandas as pd
from calculate_high_frequent_factor.common_func import *


#%%
"""
1 海通证券-海通证券选股因子系列研究（二十五）：高频因子之已实现波动分解
2 calculate_up_ratio:高频上行波动占比
"""
def calculate_up_ratio(stock_code_str,stock_min_data,n=20,stock_tday=None):
    """
    :param stock_code_str:
    :param stock_min_data: 股票分钟数据
    :param stock_tday: 需要算因子的日期列表,if none 取 分组数据里面的日期
    :param n: 取多少天的数据算因子。
    :return: 当天的因子是包含当天的信息的。
    """
    if stock_tday is None:
        stock_tday=stock_min_data.DATETIME.apply(lambda xx: xx[:10]).drop_duplicates().tolist()
    path=r'D:\code\factor_module\factor_storage\factor0001' + '/'
    if check_stock_is_done(stock_code_str,path[:-1]):
        print(stock_code_str,' already saved!')
        return


    # stock_min_data.loc[:,'DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
    # stock_min_data_trim=stock_min_data[stock_min_data['DATE'].isin(stock_tday)].copy()
    # stock_min_data_trim.loc[:,'min_return'] = stock_min_data_trim['CLOSE'].pct_change()

    stock_min_data.loc[:,'DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
    stock_min_data.loc[:,'min_return'] = stock_min_data['CLOSE'].pct_change()

    result_series=pd.Series()

    for date in stock_tday[n:]:
        print(date)
        date_index=stock_tday.index(date)
        date_list=stock_tday[date_index-n+1:date_index+1]
        data_in_date_list=stock_min_data[stock_min_data['DATE'].isin(date_list)]
        numerator=data_in_date_list[data_in_date_list['min_return']>0]['min_return'].pow(2).sum()
        denominator=data_in_date_list['min_return'].pow(2).sum()
        try:
            result_series[date]=numerator/denominator
        except Exception as e:
            print(e)
            result_series[date]=np.NaN
        finally:
            pass
        pass
    result_series.index.name='time'
    result_series.name='factor_value'
    result_series.to_frame().to_csv(path + stock_code_str + '.csv')
    return




