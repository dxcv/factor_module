"""
BCVP & OCVP
1 见微知著：成交量占比高频因子解析
2 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""
import pandas as pd


def calculate_ocvp_bcvp(stock_min_data, stock_tday):
    morning_open_time_lt = [date + ' 09:31:00' for date in stock_tday]
    morning_close1_lt = [date + ' 14:56:00' for date in stock_tday]
    morning_close2_lt = [date + ' 14:57:00' for date in stock_tday]
    morning_close3_lt = [date + ' 14:58:00' for date in stock_tday]
    morning_close4_lt = [date + ' 14:59:00' for date in stock_tday]
    morning_close5_lt = [date + ' 15:00:00' for date in stock_tday]

    ovol = stock_min_data.loc[morning_open_time_lt, 'VOLUME']
    ovol.index = stock_tday
    bvol_1 = stock_min_data.loc[morning_close1_lt, 'VOLUME']
    bvol_1.index = stock_tday
    bvol_2 = stock_min_data.loc[morning_close2_lt, 'VOLUME']
    bvol_2.index = stock_tday
    bvol_3 = stock_min_data.loc[morning_close3_lt, 'VOLUME']
    bvol_3.index = stock_tday
    bvol_4 = stock_min_data.loc[morning_close4_lt, 'VOLUME']
    bvol_4.index = stock_tday
    bvol_5 = stock_min_data.loc[morning_close5_lt, 'VOLUME']
    bvol_5.index = stock_tday
    bvol = bvol_1 + bvol_2 + bvol_3 + bvol_4 + bvol_5
    stock_min_data['date'] = pd.Series(stock_min_data.index, index=stock_min_data.index).apply(lambda x: x[:10])
    day_vol = stock_min_data.groupby('date').sum()['VOLUME']

    ocvp = ovol / day_vol
    bcvp = bvol / day_vol
    ocvp.name = 'ocvp'
    bcvp.name = 'bcvp'
    return pd.concat([ocvp, bcvp], axis=1)