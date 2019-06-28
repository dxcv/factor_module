import pandas as pd



#%%
"""
S_SKEW
1 海通证券-海通证券选股因子系列研究（二十五）：高频因子之已实现波动分解
2 提前1日
3 计算单只股票
4 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
5 需要中间变量 fama_data
"""
def calculate_s_skew(stock_min_data, stock_tday, index_data):
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

#%%