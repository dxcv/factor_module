"""
NCV & PCV & TCV
1 光大证券-光大证券多因子系列报告之七：基于K线最短路径构造的非流动性因子
2 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""
import pandas as pd

def calculate_tcv_ncv(stock_min_data):
    stock_min_data_copy = stock_min_data.copy()
    stock_min_data_copy['O2C_RATE'] = stock_min_data_copy['CLOSE']/stock_min_data_copy['OPEN']-1
    stock_min_data_copy['CONSISTENT_TRADE'] = abs(stock_min_data_copy['CLOSE']-stock_min_data_copy['OPEN'])>=0.95*(stock_min_data_copy['HIGH']-stock_min_data_copy['LOW'])
    stock_min_data_copy['TCV'] = stock_min_data_copy['CONSISTENT_TRADE']*stock_min_data_copy['VOLUME']
    #stock_min_data_copy['PCV'] = ((stock_min_data_copy['O2C_RATE']>=0)& (stock_min_data_copy['CONSISTENT_TRADE']))*stock_min_data_copy['VOLUME']
    stock_min_data_copy['NCV'] = ((stock_min_data_copy['O2C_RATE']<0) & (stock_min_data_copy['CONSISTENT_TRADE']))*stock_min_data_copy['VOLUME']
    grouped_data = stock_min_data_copy[['TCV','NCV','VOLUME','DATE']].groupby('DATE').sum()
    ncv = grouped_data['NCV']/grouped_data['VOLUME']
    tcv = grouped_data['TCV']/grouped_data['VOLUME']
    ncv.name = 'ncv'
    tcv.name = 'tcv'
    return pd.concat([ncv, tcv], axis=1)