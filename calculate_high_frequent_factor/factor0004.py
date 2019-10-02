"""
ILLIQ
1 光大证券-光大证券多因子系列报告之七：基于K线最短路径构造的非流动性因子
2 输入单只股票分钟数据（pd.DataFrame），返回日数据（pd.Series）
"""

def calculate_illiq_min(stock_min_data):
    stock_min_data_copy = stock_min_data.copy()
    stock_min_data_copy['SHORT_CUT'] =2*(stock_min_data_copy['HIGH']-stock_min_data_copy['LOW']) - abs(stock_min_data_copy['OPEN']-stock_min_data_copy['CLOSE'])
    stock_min_data_copy['ILLIQ'] = stock_min_data_copy['SHORT_CUT'] / stock_min_data_copy['AMOUNT']
    illiq = stock_min_data_copy[['ILLIQ','DATE']].groupby('DATE').sum()['ILLIQ']
    illiq.name = 'illiq_min'
    return illiq