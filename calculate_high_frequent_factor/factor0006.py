"""
R0-R4
1 方正证券-方正证券“聆听高频世界的声音”系列研究（七）：枯树生花，基于日内模式的动量因子革新 OPT_MONT中间变量
2 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""
import pandas as pd

def calculate_r0_to_4(stock_min_data, stock_tday, stock_adj):
    time1_lt = [date + ' 09:31:00' for date in stock_tday]
    time2_lt = [date + ' 10:30:00' for date in stock_tday]
    time3_lt = [date + ' 11:30:00' for date in stock_tday]
    time4_lt = [date + ' 14:00:00' for date in stock_tday]
    time5_lt = [date + ' 15:00:00' for date in stock_tday]

    time1_close = stock_min_data.loc[time1_lt, 'CLOSE']
    time2_close = stock_min_data.loc[time2_lt, 'CLOSE']
    time3_close = stock_min_data.loc[time3_lt, 'CLOSE']
    time4_close = stock_min_data.loc[time4_lt, 'CLOSE']
    time5_close = stock_min_data.loc[time5_lt, 'CLOSE']

    time1_close.index = stock_tday
    time2_close.index = stock_tday
    time3_close.index = stock_tday
    time4_close.index = stock_tday
    time5_close.index = stock_tday
    stock_day_adj = stock_adj / stock_adj.shift(1)
    yesterday_close = time5_close.shift(1) / stock_day_adj

    r0 = time1_close / yesterday_close - 1
    r1 = time2_close / time1_close - 1
    r2 = time3_close / time2_close - 1
    r3 = time4_close / time3_close - 1
    r4 = time5_close / time4_close - 1

    r0.name = 'r0'
    r1.name = 'r1'
    r2.name = 'r2'
    r3.name = 'r3'
    r4.name = 'r4'

    return pd.concat([r0, r1, r2, r3, r4], axis=1)