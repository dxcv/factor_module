# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import sys
import statsmodels.api as sm

sys.path.append('C:/code/work_space/import_moduals')
# import read_database as rb
# import matplotlib.pyplot as plt
# import datetime


# %% barra数据读取


daily_data = pd.read_pickle('D:/code/tick_data_handle/factor_test/day_data_series.pkl').to_dict()
ipo_day = pd.read_pickle('D:/code/tick_data_handle/factor_test/day_ipo.pkl')
ind_lt = pd.read_pickle('D:/code/tick_data_handle/factor_test/ind_series.pkl').values.tolist()
day_free_value=pd.read_pickle('D:/code/tick_data_handle/factor_test/mkt_freeshares.pkl')
stock_500_component = pd.read_pickle('D:/code/tick_data_handle/data/stock_500_component.pkl')
index_data = pd.read_pickle('D:/code/tick_data_handle/factor_test/ind_series.pkl').to_dict()

tday=pd.read_pickle('D:/code/tick_data_handle/factor_test/tday.pkl')
day_tday=tday.iloc[0]
week_tday=tday.iloc[1]
month_tday=tday.iloc[2]
quar_tday=tday.iloc[3]


# industry_list = rb.get_industry_list('中信一级')
day_close = daily_data['close'] * daily_data['adjfactor']
day_open = daily_data['open'] * daily_data['adjfactor']
daily_data['adj_close'] = daily_data['close'] * daily_data['adjfactor']
daily_data['adj_open'] = daily_data['open'] * daily_data['adjfactor']
day_ind = daily_data['industry']
day_ind.fillna('None', inplace=True)
day_status = daily_data['trade_status']
day_st = daily_data['is_st']
day_st = day_st.isnull() == 0


#%%
tick_factor_MIDF=pd.read_pickle('D:/code/tick_data_handle/hdf_to_factor/force_compare_ratio.pkl')
tick_factor_dict={}
for factor in tick_factor_MIDF.columns:
    tick_factor_df = tick_factor_MIDF.loc[:,factor].unstack().T
    tick_factor_df.columns = list(
        map(lambda x: x[:7] + 'SZ' if x[-1] == 'E' else x[:7] + 'SH', tick_factor_df.columns.tolist()))
    tick_factor_df.dropna(how='all', inplace=True)
    tick_factor_dict[factor]=tick_factor_df
    pass

# %% standardise
def standardise_by_benchmark(factor_df, bench_mark_weight=stock_500_component,
                             ipo_day=ipo_day, day_status=day_status, day_st=day_st):
    date_range=factor_df.index
    cond_1 = ipo_day.loc[date_range] >90
    cond_2 = day_status.loc[date_range] == 1
    cond_3 = day_st.loc[date_range] == 0
    avl_cond = cond_1 & cond_2 & cond_3
    avl_cond.dropna(how='all',inplace=True,axis=1)
    factor_df=factor_df.loc[avl_cond.index,avl_cond.columns]
    filter_factor_df=factor_df[avl_cond]

    middle_series = filter_factor_df.quantile(0.5, axis=1)
    dm1 = abs(filter_factor_df.sub(middle_series, axis=0)).quantile(0.5, axis=1)
    adj_factor: pd.DataFrame = filter_factor_df.copy()
    upper_bound = middle_series.add(5 * dm1)
    lower_bound = middle_series.add(-5 * dm1)
    adj_factor.mask(cond=adj_factor.gt(upper_bound, axis=0), other=upper_bound, axis=0, inplace=True)
    adj_factor.mask(cond=adj_factor.lt(lower_bound, axis=0), other=lower_bound, axis=0, inplace=True)

    # # 标准化
    bench_mark_weight_copy=bench_mark_weight.copy().loc[date_range]
    bench_mark_weight_copy=bench_mark_weight_copy.loc[adj_factor.index,adj_factor.columns]
    weight_sum=bench_mark_weight_copy[adj_factor.notnull()].sum(axis=1)
    bench_mark_weight_new=bench_mark_weight_copy[adj_factor.notnull()].div(weight_sum,axis=0)

    benchmark_weighted_mean = bench_mark_weight_new.mul(adj_factor,axis=1).sum(axis=1)
    std_daily_factor = adj_factor.sub(benchmark_weighted_mean,axis=0).div(adj_factor.std(axis=1),axis=0)

    return std_daily_factor

# standardise_by_benchmark(factor,bench_mark_weight=stock_500_component)


def get_industry_dummy(stock_list, date):
    industry_data = daily_data['industry']
    # 获取指定日期的，指定股票列表的行业字符窜
    day_industry_series = industry_data.loc[date, stock_list]
    industry_factor_df = pd.get_dummies(day_industry_series)
    industry_factor_df = industry_factor_df[industry_factor_df.sum(axis=1) == 1]
    industry_factor_df=industry_factor_df.loc[:,list(filter(lambda x: 'C' in x,industry_factor_df.columns.tolist()))]
    # industry_factor_df.sort_index(axis=0,inplace=True)
    return industry_factor_df

# get_industry_dummy(factor_df.columns[:100],'2018-10-12')


# 获得行业权重
def get_benchmark_ind_weight(date, benchmark_weight=stock_500_component):
    benchmark_weight_series=benchmark_weight.loc[date]
    industry_data_series = daily_data['industry'].loc[date]
    concat_df=pd.DataFrame()
    concat_df['benchmark_ind_weight']=benchmark_weight_series
    concat_df['industry']=industry_data_series
    ind_weight=concat_df.groupby('industry').sum()
    ind_weight=ind_weight.loc[list(filter(lambda x: 'C' in x,ind_weight.index.tolist()))]
    return ind_weight

# get_benchmark_ind_weight('2018-04-12')

# 获得因子收益率
def get_factor_return(date, factor_dict, bench_mark=stock_500_component, mrk_data=day_free_value,
              open_price=day_open, close_price=day_close):
    trade_day=stock_500_component.index.tolist()
    std_factor_df=pd.DataFrame()
    for factor_name in factor_dict.keys():
        std_factor_df[factor_name]=factor_dict[factor_name].loc[date, :]
    ind_factor = get_industry_dummy(list(std_factor_df.index), date)
    factor_concat = pd.concat([ind_factor, std_factor_df], axis=1)
    factor_concat['intercept']=1

    signal_date_idx = trade_day.index(date)
    signal_week_idx = week_tday.index(date)
    buy_date = trade_day[signal_date_idx+1]
    sell_date = week_tday[signal_week_idx+1]


    stock_return_rate = close_price.loc[sell_date, factor_concat.index] / open_price.loc[buy_date, factor_concat.index] - 1
    factor_concat['stock_return_rate']=stock_return_rate
    stock_return_rate.name='stock_return_rate'
    benchmark_ind_weight = get_benchmark_ind_weight(date, bench_mark)

    factor_concat = factor_concat.append(benchmark_ind_weight.T)
    factor_concat.loc['benchmark_ind_weight']=factor_concat.loc['benchmark_ind_weight'].fillna(0)

    factor_concat['regress_weight'] = mrk_data.loc[date,:].apply(np.sqrt)
    factor_concat.loc['benchmark_ind_weight','regress_weight']=1e20
    factor_concat.dropna(inplace=True)

    regress_x=factor_concat[factor_concat.columns.difference(['stock_return_rate','regress_weight'])]
    regress_y=factor_concat['stock_return_rate']
    regress_weight=factor_concat['regress_weight']

    model=sm.WLS(regress_y,regress_x,regress_weight)
    result=model.fit()
    pass

    # weight.append(1e10)
    # weight_series = pd.Series(weight, index=factor_concat.index.tolist().append(1e10))
    # weight_df = np.diag(weight)
    # xwx = ((endog.T).dot(weight_df)).dot(endog)
    # xwx_inverse = np.linalg.pinv(xwx)
    # pofolio_weight = (xwx_inverse.dot(endog.T)).dot(weight_df)
    # pofolio_weight = pd.DataFrame(pofolio_weight, columns=endog_df.index, index=endog_df.columns)
    # pofolio_weight = pofolio_weight.iloc[:, :-1]
    # factor_rate = pofolio_weight.dot(stock_return_rate)
    # predict_return = factor_concat.dot(factor_rate)
    # residual = stock_return_rate - predict_return
    #        print(predict_return)

    # return (pofolio_weight, factor_rate, factor_concat, residual, endog_df, weight_series)
    return 0

get_factor_return('2018-03-09', tick_factor_dict)

# # 设定时间
# ## st 选择在zz500有权重的日子。
# st = '2007-02-01'
# ed = '2018-04-23'
# time_ed = '2019-06-01'
# barra_std_dict = {}
# for date in barra_fillna_pl.major_axis:
#     print(date)
#     barra_std_dict[date] = standardise_by_benchmark(date, barra_fillna_pl, bench_mark_weight=day_free_value)
# barra_std_pl = pd.Panel(barra_std_dict)
# barra_std_pl = barra_std_pl.transpose(2, 0, 1)
# barra_std_pl.to_pickle('barra_std_pl.pkl', protocol=-1)
# barra_std_pl = pd.read_pickle('barra_std_pl.pkl')

# %% 大类因子合成
# barra_std_pl['residual_volatlity'] = 0.74 * barra_std_pl['dastd'] + 0.16 * barra_std_pl['cmra'] + 0.10 * barra_std_pl[
#     'hsigma']
# barra_std_pl['liquidity'] = 0.35 * barra_std_pl['stom'] + 0.35 * barra_std_pl['stoq'] + 0.30 * barra_std_pl['stoa']
# barra_std_pl['earning_yield'] = 0.68 * barra_std_pl['epibs'] + 0.21 * barra_std_pl['cetop'] + 0.11 * barra_std_pl[
#     'etop']
# barra_std_pl['growth'] = 0.18 * barra_std_pl['egib'] + 0.11 * barra_std_pl['egib_s'] + 0.24 * barra_std_pl[
#     'egro'] + 0.47 * barra_std_pl['sgro']
# barra_std_pl['leverage'] = 0.38 * barra_std_pl['mlev'] + 0.35 * barra_std_pl['dtoa'] + 0.27 * barra_std_pl['blev']
# ## delete irrelevant factor
# delete_factor = ['dastd', 'cmra', 'hsigma', 'stom', 'stoq', 'stoa', 'epibs', 'cetop',
#                  'etop', 'egib', 'egib_s', 'egro', 'sgro', 'mlev', 'dtoa', 'blev']
#
# for factor in delete_factor:
#     barra_std_pl.drop(labels=factor, axis=0, inplace=True)
#     pass
# barra_std_pl.to_pickle('barra_combine_pl.pkl', protocol=-1)
# barra_combine_pl = pd.read_pickle('barra_combine_pl.pkl')



# # %%
# # 创造行业哑变量
# def get_industry(stock_list, date, indusrty_data=day_ind, ind_list=industry_list):
#     # 获取指定日期的，指定股票列表的行业字符窜
#     day_industry_list = indusrty_data[stock_list].ix[date]
#     all_vector = []
#     # 遍历所有股票
#     for i in range(len(day_industry_list)):
#         # 创建长度等于所有行业的零向量
#         industry_vector = [0] * len(ind_list)
#         # 如果行业为空值，则插入空值向量，如果不为空则将向量对应行业位置赋值1standardise_by_benchmark
#         if (day_industry_list[i] is None) or (day_industry_list[i] == 'None'):
#             all_vector.append([np.nan] * len(ind_list))
#         else:
#             industry_vector[industry_list.index(day_industry_list[i])] = 1
#             all_vector.append(industry_vector)
#     # 将所有股票的行业向量合并为DataFrame
#     all_vector = pd.DataFrame(all_vector, columns=industry_list)
#     all_vector.index = day_industry_list.index
#     print(all_vector)
#     return all_vector
#
# # 获得行业权重
# def get_benchmark_ind_weight(date, benchmark_weight=stock_500_component, mrk_data=day_free_value):
#     stock_list = benchmark_weight.loc[date][np.isnan(benchmark_weight.loc[date]) == 0].index
#     ind_factor = get_industry(stock_list, date)
#     ind_factor = ind_factor.fillna(value=0)
#     daily_mrk = mrk_data.loc[date, stock_list]
#     ind_mar = daily_mrk.dot(ind_factor)
#     ind_mar = ind_mar / (ind_mar.sum())
#     return ind_mar
#
# def get_endog(date, factor_pl, bench_mark=stock_500_component, universe=day_free_value, mrk_data=day_free_value,
#               open_data=day_open, close_date=day_close, trade_day=trade_day_list):
#     #    std_factor_df = standardise_by_benchmark(date, factor_pl, bench_mark,universe)
#     std_factor_df = factor_pl.loc[:, date, :]
#     ind_factor = get_industry_dummy(list(std_factor_df.index), date)
#     factor = pd.concat([ind_factor, std_factor_df], axis=1)
#     factor = factor.loc[factor.isna().sum(axis=1) == 0]
#
#     '''signal_date_idx = trade_day.index(date)
#     signal_week_idx = week_tday.index(date)
#     buy_date = trade_day[signal_date_idx+1]
#     sell_date = week_tday[signal_week_idx+1]'''
#     signal_date_idx = trade_day.index(date)
#     buy_date = date
#     sell_date = trade_day[signal_date_idx + 1]
#     print(buy_date, sell_date)
#     # print(date)
#
#     if date == close_date.index[-1]:
#         benchmark_ind_value = get_benchmark_ind_weight(date, bench_mark).values
#         factor.insert(0, 'intercept', [1] * len(factor.index))
#         constrain = [0] + list(benchmark_ind_value) + [0] * len(std_factor_df.columns)
#         constrain = pd.DataFrame(constrain, index=factor.columns, columns=['constrain']).T
#         endog_df = pd.concat([factor, constrain])
#         endog = endog_df.values
#         weight = list(np.sqrt(mrk_data.loc[date, factor.index]))
#         weight.append(1e10)
#         weight_series = pd.Series(weight, index=factor.index.tolist().append(1e10))
#         weight_df = np.diag(weight)
#         xwx = ((endog.T).dot(weight_df)).dot(endog)
#         xwx_inverse = np.linalg.pinv(xwx)
#         pofolio_weight = (xwx_inverse.dot(endog.T)).dot(weight_df)
#         pofolio_weight = pd.DataFrame(pofolio_weight, columns=endog_df.index, index=endog_df.columns)
#         pofolio_weight = pofolio_weight.iloc[:, :-1]
#         factor_rate = pd.Series(np.nan, index=pofolio_weight.index)
#         predict_return = factor.dot(factor_rate)
#         residual = pd.Series(np.nan, index=pofolio_weight.columns)
#     #        print(predict_return)
#
#     else:
#         # rate = close_date.loc[sell_date,factor.index]/open_data.loc[buy_date,factor.index]-1
#         rate = close_date.loc[sell_date, factor.index] / close_date.loc[buy_date, factor.index] - 1
#         rate = rate[rate.isna() == 0]
#         factor = factor.loc[rate.index]
#         benchmark_ind_value = get_benchmark_ind_weight(date, bench_mark, universe).values
#         factor.insert(0, 'intercept', [1] * len(factor.index))
#         constrain = [0] + list(benchmark_ind_value) + [0] * len(std_factor_df.columns)
#         constrain = pd.DataFrame(constrain, index=factor.columns, columns=['constrain']).T
#         endog_df = pd.concat([factor, constrain])
#         endog = endog_df.values
#         weight = list(np.sqrt(mrk_data.loc[date, factor.index]))
#         weight.append(1e10)
#         weight_series = pd.Series(weight, index=factor.index.tolist().append(1e10))
#         weight_df = np.diag(weight)
#         xwx = ((endog.T).dot(weight_df)).dot(endog)
#         xwx_inverse = np.linalg.pinv(xwx)
#         pofolio_weight = (xwx_inverse.dot(endog.T)).dot(weight_df)
#         pofolio_weight = pd.DataFrame(pofolio_weight, columns=endog_df.index, index=endog_df.columns)
#         pofolio_weight = pofolio_weight.iloc[:, :-1]
#         factor_rate = pofolio_weight.dot(rate)
#         predict_return = factor.dot(factor_rate)
#         residual = rate - predict_return
#     #        print(predict_return)
#
#     return (pofolio_weight, factor_rate, factor, residual, endog_df, weight_series)
#
#
# def plot_factor_return(factor_pl, start_date, end_date, universe=day_free_value, bench_mark=stock_500_component,
#                        trade_day=trade_day_list):
#     signal_date_lt = list(factor_pl.loc[:, start_date:end_date, :].major_axis)
#
#     all_date_residual = []
#     all_date_rate = []
#     all_date_weight = {}
#     all_date_factor = {}
#     all_date_stock_weight = []
#     all_date_endog = {}
#
#     for date in signal_date_lt:
#         # week_idx = week_tday.index(date)
#         date_idx = trade_day.index(date)
#         daily_endog = get_endog(date, factor_pl, bench_mark=bench_mark, universe=universe)
#
#         daily_weight = daily_endog[0]
#         daily_rate = daily_endog[1]
#         daily_factor = daily_endog[2]
#         daily_residual = daily_endog[3]
#         daily_endog_df = daily_endog[4]
#         daily_stock_weight_series = daily_endog[5]
#
#         daily_stock_weight_series.name = trade_day[date_idx + 1]
#         daily_rate.name = trade_day[date_idx + 1]
#         daily_residual.name = trade_day[date_idx + 1]
#         #        daily_rate.name = date
#         all_date_rate.append(daily_rate)
#         all_date_residual.append(daily_residual)
#         all_date_weight[date] = daily_weight
#         all_date_factor[date] = daily_factor
#         all_date_stock_weight.append(daily_stock_weight_series)
#         all_date_endog[date] = daily_endog_df
#
#     all_date_residual_df = pd.concat(all_date_residual, axis=1).T
#     all_date_rate_df = pd.concat(all_date_rate, axis=1).T
#     all_date_weight_panel = pd.Panel(all_date_weight)
#     all_date_factor_panel = pd.Panel(all_date_factor)
#     all_date_endog_panel = pd.Panel(all_date_endog)
#     all_date_stock_weight_df = pd.concat(all_date_stock_weight, axis=1).T
#
#     x_axis = all_date_rate_df.index.values
#     x_axis_dt = []
#     for i in range(len(x_axis)):
#         x_axis_dt.append(datetime.datetime.strptime(x_axis[i], '%Y-%m-%d'))
#
#     fig = plt.figure()
#
#     fig.set_size_inches(70, 35)
#     bar_plot = {}
#     num = 1
#     for i in list(all_date_rate_df.columns)[-10:]:
#         bar_plot[i] = fig.add_subplot(4, 3, num)
#         num += 1
#
#     for i in bar_plot.keys():
#         bar_plot[i].bar(x_axis_dt, all_date_rate_df[i], label=i, width=5)
#         bar_plot[i].legend(bbox_to_anchor=(1.0, 1), loc=1, borderaxespad=0.)
#
#     plt.show()
#
#     return (all_date_rate_df, all_date_weight_panel, all_date_factor_panel,
#             all_date_residual_df, all_date_endog_panel, all_date_stock_weight_df)
