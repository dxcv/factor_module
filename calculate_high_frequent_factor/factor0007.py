"""
VR
1 高频因子：日内分时成交量蕴藏玄机
2 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""
import pandas as pd


def calculate_vr(stock_min_data, stock_tday):
    stock_min_vol = stock_min_data.loc[stock_tday[0] + ' 09:00:00':stock_tday[-1] + ' 15:30:00', 'VOLUME']
    morning_idx = []
    noon_idx = []
    for i in range(len(stock_tday)):
        for j in range(30):
            morning_idx.append(j + i * 240)
            noon_idx.append(120 + j + i * 240)

    morning_vol = stock_min_vol.iloc[morning_idx]
    noon_vol = stock_min_vol.iloc[noon_idx]
    date_ser = pd.Series(morning_vol.index, index=morning_vol.index).apply(lambda x: x[:10])
    morning_vol = pd.concat([morning_vol, date_ser], axis=1)
    date_ser.index = noon_vol.index
    noon_vol = pd.concat([noon_vol, date_ser], axis=1)
    morning_vol.columns = ['VOLUME', 'DATE']
    noon_vol.columns = ['VOLUME', 'DATE']
    day_morning_vol = morning_vol.groupby('DATE').sum()['VOLUME']
    day_noon_vol = noon_vol.groupby('DATE').sum()['VOLUME']
    vr = day_morning_vol / day_noon_vol
    vr.name = 'vr'
    return vr