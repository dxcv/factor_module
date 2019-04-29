import pandas as pd

"""
SMB HML
http://www.microbell.com/docdetail_2178839.html
1 海通证券-海通证券选股因子系列研究（二十五）：高频因子之已实现波动分解
2 提前1日
3 计算整个市场
4 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）SMB HML
"""


# %%
def get_smb_hml(day_size=day_free_size, pb=pb, wind_stock_lt=wind_stock_lt, day_adj=day_adj,
                min_data_tday=min_data_tday):
    wind_stock_lt = wind_stock_lt[wind_stock_lt.isin(day_size.columns)]
    size_small_cond = day_size.le(day_size.quantile(0.333, axis=1), axis='index')
    size_great_cond = day_size.gt(day_size.quantile(0.666, axis=1), axis='index')
    pb_small_cond = pb.le(pb.quantile(0.333, axis=1), axis='index')
    pb_great_cond = pb.gt(pb.quantile(0.666, axis=1), axis='index')

    size_small_cond = size_small_cond[wind_stock_lt]
    size_great_cond = size_great_cond[wind_stock_lt]
    pb_small_cond = pb_small_cond[wind_stock_lt]
    pb_great_cond = pb_great_cond[wind_stock_lt]

    smb = pd.Series()
    hml = pd.Series()
    adj_data = (day_adj / day_adj.shift(1))[wind_stock_lt]

    with open('E:/work_space/pure_factor_deployment/fama_data.pkl', 'rb') as handle:
        fama_data_old = pickle.load(handle)
    st_date = fama_data_old.index[-1][:10]

    if st_date == tday_lt[-1]:
        return fama_data_old
    else:
        min_data_tday = tday_lt[tday_lt.index(st_date) + 1:tday_lt.index(min_ed) + 1]
        print(min_data_tday)

        for date in min_data_tday:
            t1 = time.time()
            size_date_idx = list(day_size.index).index(date)
            pd_date_idx = list(pb.index).index(date)
            daily_size_small_cond = size_small_cond.iloc[size_date_idx - 1]
            daily_size_great_cond = size_great_cond.iloc[size_date_idx - 1]
            daily_pb_small_cond = pb_small_cond.iloc[pd_date_idx - 1]
            daily_pb_great_cond = pb_great_cond.iloc[pd_date_idx - 1]
            last_date = size_small_cond.index[size_date_idx - 1]
            daily_min_close = rmb.get_date_all_stock_data(last_date + ' 14:59:00', date + ' 15:30:00', 'CLOSE')
            t2 = time.time()
            print(date, t2 - t1)
            daily_adj = adj_data.loc[date, daily_min_close.columns]
            daily_min_close.iloc[0] = daily_min_close.iloc[0].divide(daily_adj)
            daily_rate = ((daily_min_close / daily_min_close.shift(1)) - 1).iloc[-240:]
            small_size_group_rate = daily_rate.T[daily_size_small_cond].T.mean(axis=1)
            big_size_group_rate = daily_rate.T[daily_size_great_cond].T.mean(axis=1)
            small_pb_group_rate = daily_rate.T[daily_pb_small_cond].T.mean(axis=1)
            big_pb_group_rate = daily_rate.T[daily_pb_great_cond].T.mean(axis=1)
            daily_smb = small_size_group_rate - big_size_group_rate
            daily_hml = small_pb_group_rate - big_pb_group_rate
            smb = pd.concat([smb, daily_smb])
            hml = pd.concat([hml, daily_hml])
        fama_data = pd.concat([smb, hml], axis=1)
        fama_data.columns = ['smb', 'hml']
        all_fama_data = pd.concat([fama_data_old, fama_data])

        with open('E:/work_space/pure_factor_deployment/fama_data.pkl', 'wb') as handle:
            pickle.dump(all_fama_data, handle, pickle.HIGHEST_PROTOCOL)

    return all_fama_data


fama_data_df = get_smb_hml()

#%%
"""
S_SKEW
1 海通证券-海通证券选股因子系列研究（二十五）：高频因子之已实现波动分解
2 提前1日
3 计算单只股票
4 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
5 需要中间变量 fama_data
"""
def calculate_s_skew(stock_min_data, stock_tday, index_data, fama_data=fama_data):
    smb = fama_data['smb']
    hml = fama_data['hml']
    stock_data = stock_min_data['CLOSE']
    stock_rate = (stock_data / stock_data.shift(1)) - 1
    index_rate = (index_data / index_data.shift(1)) - 1
    stock_vol = pd.Series()
    stock_skew = pd.Series()
    # stock_kurt = pd.Series()

    for i in range(1, len(stock_tday)):
        date = stock_tday[i]
        daily_rate = stock_rate.loc[date + ' 09:32:00':date + ' 15:30:00']
        daily_mkt = index_rate.loc[date + ' 09:32:00':date + ' 15:30:00']
        daily_smb = smb.loc[date + ' 09:32:00':date + ' 15:30:00']
        daily_hml = hml.loc[date + ' 09:32:00':date + ' 15:30:00']
        intercept = pd.Series(1, index=daily_rate.index)
        x = pd.concat([intercept, daily_mkt, daily_smb, daily_hml], axis=1).values
        y = daily_rate.values
        xx = (x.T).dot(x)
        xx_inv = np.linalg.pinv(xx)
        result = xx_inv.dot(x.T).dot(y)
        est = x.dot(result)
        residual = pd.Series(y - est)
        idvol = np.sqrt((residual ** 2).sum())
        idskew = (np.sqrt(len(residual)) * ((residual ** 3).sum())) / (idvol ** (3))
        # idkurt = (len(residual)*((residual**4).sum()))/(idvol**4)
        stock_vol.loc[date] = idvol
        stock_skew.loc[date] = idskew
        # stock_kurt.loc[date] = idkurt

    stock_skew.name = 's_skew'
    return stock_skew