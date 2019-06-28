"""
STAT
1 凤鸣朝阳：股价日内模式中蕴藏的选股因子 APM 中间因子
2 提前20日
3 计算单只股票
4 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""

import numpy as np
from data_base.mongodb import MongoDB_io
import pandas as pd
import statsmodels.api as sm
import collections
import matplotlib.pyplot as plt

#%%

def calculate_stat(stock_min_data, stock_tday, index_min_data, n=20):
    stock_min_data['DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
    index_min_data['DATE']=index_min_data.DATETIME.apply(lambda xx:xx[:10])
    regress_df=pd.DataFrame()

    morning_open_time_lt = [date + ' 09:31:00' for date in stock_tday]
    morning_close_time_lt = [date + ' 11:30:00' for date in stock_tday]
    noon_open_time_lt = [date + ' 13:01:00' for date in stock_tday]
    noon_close_time_lt = [date + ' 15:00:00' for date in stock_tday]

    # 求二十天收益率
    n_day_pair=collections.OrderedDict(zip(morning_open_time_lt[:-n+1],noon_close_time_lt[n-1:]))
    start_day=stock_min_data[stock_min_data.DATETIME.isin(n_day_pair.keys())]
    start_day.reset_index(inplace=True)
    end_day=stock_min_data[stock_min_data.DATETIME.isin(n_day_pair.values())]
    end_day.reset_index(inplace=True)
    n_day_return=end_day['CLOSE']-start_day['OPEN']
    n_day_return.index=[x[:10] for x in n_day_pair.values()]

    # 股票早上收益率
    morning_open:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(morning_open_time_lt)]
    morning_open.set_index('DATE',inplace=True)
    morning_open_series=morning_open['OPEN']
    morning_close:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(morning_close_time_lt)]
    morning_close.set_index('DATE',inplace=True)
    morning_close_series=morning_open['CLOSE']
    ram = morning_close_series / morning_open_series - 1
    # 股票下午收益率
    noon_open:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(noon_open_time_lt)]
    noon_open.set_index('DATE',inplace=True)
    noon_open_series=noon_open['OPEN']
    noon_close:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(noon_close_time_lt)]
    noon_close.set_index('DATE',inplace=True)
    noon_close_series=noon_open['CLOSE']
    rpm = noon_close_series / noon_open_series - 1
    # 指数上午收益率
    index_morning_open:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(morning_open_time_lt)]
    index_morning_open.set_index('DATE',inplace=True)
    index_morning_open_series=index_morning_open['OPEN']
    index_morning_close:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(morning_close_time_lt)]
    index_morning_close.set_index('DATE',inplace=True)
    index_morning_close_series=index_morning_open['CLOSE']
    Ram = index_morning_close_series / index_morning_open_series - 1
    # 指数下午收益率
    index_noon_open:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(noon_open_time_lt)]
    index_noon_open.set_index('DATE',inplace=True)
    index_noon_open_series=index_noon_open['OPEN']
    index_noon_close:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(noon_close_time_lt)]
    index_noon_close.set_index('DATE',inplace=True)
    index_noon_close_series=index_noon_open['CLOSE']
    Rpm = index_noon_close_series / index_noon_open_series - 1

    regress_df['ram']=ram
    regress_df['rpm']=rpm
    regress_df['Ram']=Ram
    regress_df['Rpm']=Rpm
    regress_df['n_day_return']=n_day_return
    # regress_df['num']=range(regress_df.shape[0])

    # 回归早上
    X=regress_df[['Ram']]
    X=sm.add_constant(X)
    Y=regress_df['ram']
    model=sm.OLS(Y,X).fit()
    regress_df['resid_am']=model.resid
    # 回归下午
    X=regress_df[['Rpm']]
    X=sm.add_constant(X)
    Y=regress_df['rpm']
    model=sm.OLS(Y,X).fit()
    regress_df['resid_pm']=model.resid
    # diff
    regress_df['resid_diff']=regress_df['resid_am']-regress_df['resid_pm']
    #
    regress_df['stat']=regress_df['resid_diff'].rolling(n).apply(lambda x:np.mean(x)/np.std(x)/n**0.5)
    # regress apm
    X=regress_df[['n_day_return']].dropna()
    Y=regress_df['stat'].dropna()
    model=sm.OLS(Y,X).fit()
    regress_df['apm']=model.resid

    # fig, ax = plt.subplots()
    # ax.scatter(regress_df['n_day_return'], regress_df['stat'], alpha=0.5)
    # plt.show()

    # 因子值是通过当天的数据算得的
    return regress_df['apm'].dropna()

#%%

if __name__=="__main__":

    #%% 获得交易日
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_trade_date')
    trade_date_info=m.read_data_to_get_dataframe()
    trade_date=trade_date_info[(trade_date_info.date>pd.to_datetime('2018-01-01'))&(
            trade_date_info.date<pd.to_datetime('2019-01-01'))]['date'].astype(str).tolist()

    #%% 获得指数数据
    m.set_MongoClient()
    m.set_db('index_min_info')
    m.set_collection('000001')
    index_data=m.read_data_to_get_dataframe()

    #%% 获得股票代码列表
    m.set_db('stock_min_info')
    stock_code=m.list_collection_names()
    stock_code.sort()



    #%% cal_factor
    factor_df=pd.DataFrame()
    for stock in stock_code:
        try:
            print(stock)
            m.set_collection(stock)
            stock_min_data=m.read_data_to_get_dataframe()
            factor_data=calculate_stat(stock_min_data,trade_date,index_min_data=index_data)
            factor_df[stock]=factor_data
        except:
            continue
        pass
    factor_df.to_pickle(r'D:\code\factor_modual\factor_pickle\0001.pkl')

# def calculate_stat_2(stock_min_data, stock_tday, index_min_data, n=20):
#     morning_open_time_lt = [date + ' 09:31:00' for date in stock_tday]
#     morning_close_time_lt = [date + ' 11:30:00' for date in stock_tday]
#     noon_open_time_lt = [date + ' 13:01:00' for date in stock_tday]
#     noon_close_time_lt = [date + ' 15:00:00' for date in stock_tday]
#
#     morning_open = stock_min_data.loc[morning_open_time_lt, 'OPEN']
#     morning_open.index = stock_tday
#     morning_close = stock_min_data.loc[morning_close_time_lt, 'CLOSE']
#     morning_close.index = stock_tday
#     ram = morning_close / morning_open - 1
#
#     noon_open = stock_min_data.loc[noon_open_time_lt, 'OPEN']
#     noon_open.index = stock_tday
#     noon_close = stock_min_data.loc[noon_close_time_lt, 'CLOSE']
#     noon_close.index = stock_tday
#     rpm = noon_close / noon_open - 1
#
#     index_morning_open = index_min_data.loc[morning_open_time_lt, 'OPEN']
#     index_morning_open.index = stock_tday
#     index_morning_close = index_min_data.loc[morning_close_time_lt, 'CLOSE']
#     index_morning_close.index = stock_tday
#     Ram = index_morning_close / index_morning_open - 1
#
#     index_noon_open = index_min_data.loc[noon_open_time_lt, 'OPEN']
#     index_noon_open.index = stock_tday
#     index_noon_close = index_min_data.loc[noon_close_time_lt, 'CLOSE']
#     index_noon_close.index = stock_tday
#     Rpm = index_noon_close / index_noon_open - 1
#
#     stock_date_lt = list(ram.index)
#     index_date_lt = list(Ram.index)
#     stat_lt = pd.Series()
#     for i in range(n, len(ram)):
#         date = stock_date_lt[i]
#         index_date_idx = index_date_lt.index(date)
#         interval_ram = ram.iloc[i - n:i]
#         interval_rpm = rpm.iloc[i - n:i]
#         interval_Ram = Ram.iloc[index_date_idx - n:index_date_idx]
#         interval_Rpm = Rpm.iloc[index_date_idx - n:index_date_idx]
#         interval_stock = pd.concat([interval_ram, interval_rpm], axis=0).values
#         interval_index = pd.concat([interval_Ram, interval_Rpm], axis=0).values
#         x = np.column_stack((np.ones((len(interval_index),)), interval_index))
#         coef = np.linalg.pinv((x.T).dot(x)).dot(x.T).dot(interval_stock)
#         residual = np.column_stack((np.ones((len(interval_index),)), interval_stock)).dot(coef)
#         sigma_am = residual[:n]
#         sigma_pm = residual[n:]
#         diff = sigma_am - sigma_pm
#         stat = diff.mean() / (diff.std() * np.sqrt(n))
#         stat_lt[date] = stat
#     stat_lt.name = 'stat'
#
#     return stat_lt