import pandas as pd
# from data_base.mongodb import MongoDB_io,cal_time
from data_base.mongodb import MongoDB_io
from multi_thread_process_module.multi_process_module import MyProcess
from multiprocessing import Semaphore
import os

#%%
"""
1 海通证券-海通证券选股因子系列研究（二十五）：高频因子之已实现波动分解
2 calculate_up_ratio:高频上行波动占比
"""
def calculate_up_ratio(stock_min_data, stock_tday:list,n=1):
    """
    :param stock_min_data: 股票分钟数据
    :param stock_tday: 需要算因子的日期列表
    :param n: 取多少天的数据算因子。
    :return: 当天的因子是包含当天的信息的。
    """
    stock_min_data.loc[:,'DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
    stock_min_data_trim=stock_min_data[stock_min_data['DATE'].isin(stock_tday)].copy()
    stock_min_data_trim.loc[:,'min_return'] = stock_min_data_trim['CLOSE'].pct_change()

    result_series=pd.Series()

    for date in stock_tday[n:]:
        date_index=stock_tday.index(date)
        date_list=stock_tday[date_index-n+1:date_index+1]
        data_in_date_list=stock_min_data_trim[stock_min_data_trim['DATE'].isin(date_list)]
        numerator=data_in_date_list[data_in_date_list['min_return']>0]['min_return'].pow(2).sum()
        denominator=data_in_date_list['min_return'].pow(2).sum()
        try:
            result_series[date]=numerator/denominator
        except Exception as e:
            print(e)
            result_series[date]=None
        finally:
            pass
        pass
    return result_series


# def calculate_s_skew(stock_min_data, stock_tday:list,n=20):
#     stock_min_data['DATE']=stock_min_data.DATETIME.apply(lambda xx:xx[:10])
#     stock_min_data_trim=stock_min_data[stock_min_data['DATE'].isin(stock_tday)]
#
#     stock_data = stock_min_data_trim['CLOSE']
#     stock_rate = (stock_data / stock_data.shift(1)) - 1
#
#     for date in stock_tday[20:]:
#         date_index=stock_tday.index(date)
#         date_list=stock_tday[date_index-n+1:date_index]
#         stock_min_data_trim=stock_min_data_trim[stock_min_data_trim['DATE'].isin(date_list)]
#
#         pass
#
#     return stock_skew

# @ cal_time
def calculate_factor(stock_code_str,trade_date_list_):
    m_ = MongoDB_io()
    m_.set_MongoClient()
    m_.set_db('stock_min_info')
    m_.set_collection(stock_code_str)
    stock_min_data_=m_.read_data_to_get_dataframe()
    result_series=calculate_up_ratio(stock_min_data_, trade_date_list_)
    result_series.to_csv(r'D:\code\factor_module\factor_storage\factor0001\factor0001_01\s'+stock_code_str+'.csv')
    print(stock_code_str,' done')
    pass

if __name__=="__main__":

    # region Description: get trade date list
    start_date='2015-01-01'
    end_date='2019-09-01'
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_trade_date')
    # trade_date_info=m.read_data_to_get_dataframe()
    trade_date_info=m.read_data_to_get_dataframe_include_condition(start_date=start_date,end_date=end_date)
    trade_date_list=trade_date_info[(trade_date_info.date > pd.to_datetime(start_date)) & (
            trade_date_info.date<pd.to_datetime(end_date))]['date'].astype(str).tolist()
    # endregion

    # region Description: get stock list
    m.set_MongoClient()
    m.set_db('stock_min_info')
    stock_code_list=m.list_collection_names()
    stock_code_list.sort()
    # endregion

    # region Description: calculate factor
    factor_df=pd.DataFrame()


    # # region Description: use loop
    # for stock in stock_code_list:
    #     try:
    #         print(stock)
    #         m.set_collection(stock)
    #         stock_min_data_=m.read_data_to_get_dataframe()
    #         factor_data=calculate_up_ratio(stock_min_data_, trade_date_list)
    #         factor_df[stock]=factor_data
    #     except Exception as e:
    #         print(e)
    #         continue
    #     pass
    # # endregion




    # region Description:use process Semphore
    process_list=[]
    sem=Semaphore(4)
    path=r'D:\code\factor_module\factor_storage\factor0001\factor0001_01'
    file_list=os.listdir(path)
    finish_stock_list=list(map(lambda x:x[1:-4],file_list))
    stock_code_list=list(set(stock_code_list).difference(set(finish_stock_list)))
    stock_code_list.sort()

    for stock in stock_code_list:
        print(stock)
        # factor_df[stock] = process_pool.apply(calculate_factor,args=(stock,trade_date_list))
        p=MyProcess(target=calculate_factor,args=(stock,trade_date_list),kwargs={'sem':sem})
        p.daemon=True
        p.start()
        process_list.append(p)
        pass
    # endregion
    for proc in process_list:
        proc.join()

    # endregion