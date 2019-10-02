from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from multi_thread_process_module.multi_process_module import MyProcess
from multiprocessing import Semaphore


def download_and_insert(m,stock,start_date,end_date):
    min_data: pd.DataFrame = get_price(stock, start_date=start_date, end_date=end_date, frequency='minute',
                                       fields=None, skip_paused=False, fq=None, count=None)
    if min_data.shape[0]==0:
        print(stock,' is empty')
        return
    min_data.index.name = 'datetime'
    min_data.reset_index(inplace=True)
    m.set_collection(stock.replace('.', '_'))
    m.insert_huge_dataframe_by_block_to_mongodb(min_data)
    pass

def check_stock_is_in_collection(m,stock):
    collection_list=m.list_collection_names()
    if stock in list(map(lambda x:x.replace('_','.'),collection_list)):
        return True
    else:
        return False
    pass

def get_collection_insert_date(m,collection):
    m.set_collection(collection)
    df=m.read_data_to_get_field(field={'datetime':1})
    date_list = df.datetime.astype(str).apply(lambda x:x[:10]).drop_duplicates().tolist()
    date_list.sort()
    return date_list
    pass

def multi_process_insert_min_data(stock_code_list,start_date,end_date,trade_date_list):
    process_list=[]
    sem=Semaphore(4)
    for stock in stock_code_list:
        print(stock)
        p=MyProcess(target=inserting_one_stock, args=(stock, start_date, end_date, trade_date_list), kwargs={'sem':sem})
        p.daemon=True
        p.start()
        process_list.append(p)
        pass
    for proc in process_list:
        proc.join()
        pass
    pass

def single_process_insert_min_data(stock_code_list,start_date,end_date,trade_date_list):
    for stock in stock_code_list:
        print(stock)
        inserting_one_stock(stock, start_date, end_date, trade_date_list)
        pass
    pass

def inserting_one_stock(stock, start_date, end_date, trade_date_list):
    m=MongoDB_io()
    m.set_db('stock_real_min_data')
    flag=check_stock_is_in_collection(m,stock)
    if not flag:
        download_and_insert(m,stock,start_date,end_date)
    else:
        date_list=get_collection_insert_date(m,stock.replace('.','_'))
        last_insert_date=date_list[-1]
        index=trade_date_list.index(last_insert_date)
        next_trade_date=trade_date_list[index+1]
        download_and_insert(m,stock, next_trade_date, trade_date_list[-1])
    pass

def insert_stock_min_data():
    change_wording_address()
    logging_joinquant()
    stock_list=get_stock_code_list()
    stock_list.sort()
    stock_list=stock_list[0:200]
    start_date=get_setting_start_date()
    trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date)).astype(str)
    trade_date_list=trade_date_series.tolist()
    end_date=trade_date_list[-1]
    single_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    pass

def insert_stock_min_data2():
    change_wording_address()
    logging_joinquant()
    stock_list=get_stock_code_list()
    stock_list.sort()
    stock_list=stock_list[200:400]
    start_date=get_setting_start_date()
    trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date)).astype(str)
    trade_date_list=trade_date_series.tolist()
    end_date=trade_date_list[-1]
    single_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    pass

def insert_stock_min_data3():
    change_wording_address()
    logging_joinquant()
    stock_list=get_stock_code_list()
    stock_list.sort()
    stock_list=stock_list[400:600]
    start_date=get_setting_start_date()
    trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date)).astype(str)
    trade_date_list=trade_date_series.tolist()
    end_date=trade_date_list[-1]
    single_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    pass

if __name__=='__main__':
    insert_stock_min_data()