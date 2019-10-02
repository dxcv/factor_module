"""
STAT
1 凤鸣朝阳：股价日内模式中蕴藏的选股因子 APM 中间因子
2 提前20日
3 计算单只股票
4 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""

# import numpy as np
# # from data_base.mongodb import MongoDB_io
# import pandas as pd
# import statsmodels.api as sm
# import collections
#
#
# #%%
#
# def calculate_stat(stock_min_data_to_copy, stock_tday, index_min_data_to_copy, n=20):
#     stock_min_data=stock_min_data_to_copy.copy()
#     index_min_data=index_min_data_to_copy.copy()
#
#     stock_min_data['DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
#     index_min_data['DATE']=index_min_data.DATETIME.apply(lambda xx:xx[:10])
#     regress_df=pd.DataFrame()
#
#     morning_open_time_lt = [date + ' 09:31:00' for date in stock_tday]
#     morning_close_time_lt = [date + ' 11:30:00' for date in stock_tday]
#     noon_open_time_lt = [date + ' 13:01:00' for date in stock_tday]
#     noon_close_time_lt = [date + ' 15:00:00' for date in stock_tday]
#
#     # 求二十天收益率
#     n_day_pair=collections.OrderedDict(zip(morning_open_time_lt[:-n+1],noon_close_time_lt[n-1:]))
#     start_day=stock_min_data[stock_min_data.DATETIME.isin(n_day_pair.keys())]
#     start_day.reset_index(inplace=True)
#     end_day=stock_min_data[stock_min_data.DATETIME.isin(n_day_pair.values())]
#     end_day.reset_index(inplace=True)
#     n_day_return=end_day['CLOSE']-start_day['OPEN']
#     n_day_return.index=[x[:10] for x in n_day_pair.values()]
#
#     # 股票早上收益率
#     morning_open:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(morning_open_time_lt)]
#     morning_open.set_index('DATE',inplace=True)
#     morning_open_series=morning_open['OPEN']
#     morning_close:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(morning_close_time_lt)]
#     morning_close.set_index('DATE',inplace=True)
#     morning_close_series=morning_open['CLOSE']
#     ram = morning_close_series / morning_open_series - 1
#
#
#
#     # 股票下午收益率
#     noon_open:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(noon_open_time_lt)]
#     noon_open.set_index('DATE',inplace=True)
#     noon_open_series=noon_open['OPEN']
#     noon_close:pd.DataFrame = stock_min_data[stock_min_data.DATETIME.isin(noon_close_time_lt)]
#     noon_close.set_index('DATE',inplace=True)
#     noon_close_series=noon_open['CLOSE']
#     rpm = noon_close_series / noon_open_series - 1
#
#     # 指数上午收益率
#     index_morning_open:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(morning_open_time_lt)]
#     index_morning_open.set_index('DATE',inplace=True)
#     index_morning_open_series=index_morning_open['OPEN']
#     index_morning_close:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(morning_close_time_lt)]
#     index_morning_close.set_index('DATE',inplace=True)
#     index_morning_close_series=index_morning_open['CLOSE']
#     Ram = index_morning_close_series / index_morning_open_series - 1
#     # 指数下午收益率
#     index_noon_open:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(noon_open_time_lt)]
#     index_noon_open.set_index('DATE',inplace=True)
#     index_noon_open_series=index_noon_open['OPEN']
#     index_noon_close:pd.DataFrame = index_min_data[index_min_data.DATETIME.isin(noon_close_time_lt)]
#     index_noon_close.set_index('DATE',inplace=True)
#     index_noon_close_series=index_noon_open['CLOSE']
#     Rpm = index_noon_close_series / index_noon_open_series - 1
#
#     regress_df['ram']=ram
#     regress_df['rpm']=rpm
#     regress_df['Ram']=Ram
#     regress_df['Rpm']=Rpm
#     regress_df['n_day_return']=n_day_return
#     # regress_df['num']=range(regress_df.shape[0])
#
#     # 回归早上
#     X=regress_df[['Ram']]
#     X=sm.add_constant(X)
#     Y=regress_df['ram']
#     model=sm.OLS(Y,X).fit()
#     regress_df['resid_am']=model.resid
#     # 回归下午
#     X=regress_df[['Rpm']]
#     X=sm.add_constant(X)
#     Y=regress_df['rpm']
#     model=sm.OLS(Y,X).fit()
#     regress_df['resid_pm']=model.resid
#     # diff
#     regress_df['resid_diff']=regress_df['resid_am']-regress_df['resid_pm']
#     #
#     regress_df['stat']=regress_df['resid_diff'].rolling(n).apply(lambda x:np.mean(x)/np.std(x)/n**0.5)
#     # regress apm
#     X=regress_df[['n_day_return']].dropna()
#     Y=regress_df['stat'].dropna()
#     model=sm.OLS(Y,X).fit()
#     regress_df['apm']=model.resid
#
#     # fig, ax = plt.subplots()
#     # ax.scatter(regress_df['n_day_return'], regress_df['stat'], alpha=0.5)
#     # plt.show()
#
#     # 因子值是通过当天的数据算得的
#     return regress_df['apm'].dropna()

