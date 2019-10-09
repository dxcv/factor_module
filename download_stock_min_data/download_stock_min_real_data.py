from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
# from multi_thread_process_module.multi_process_module import MyProcess
# from multiprocessing import Semaphore


class get_min_price_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.m.set_db('stock_min_data')
        self.nothing=''
        self.collection_list=self.m.list_collection_names()
        pass

    def download_and_insert(self,stock,start_date,end_date):
        m=self.m
        print(stock,start_date,end_date)
        min_data: pd.DataFrame = get_price(stock, start_date=start_date, end_date=end_date, frequency='minute',
                                           fields=None, skip_paused=False, fq=None, count=None)
        if min_data.shape[0]==0:
            print(stock,' is empty')
            return
        min_data.index.name = 'datetime'
        min_data.reset_index(inplace=True)
        min_data.columns=min_data.columns.map(lambda x:x.upper())
        min_data.rename({'MONEY':'AMOUNT'},axis=1,inplace=True)
        min_data.DATETIME=min_data.DATETIME.astype(str)
        m.set_collection(stock[:6])
        # m.insert_huge_dataframe_by_block_to_mongodb(min_data)
        m.insert_dataframe_to_mongodb(min_data)
        pass

    def check_stock_is_in_collection(self,stock):
        collection_list=self.collection_list
        if stock[:6] in collection_list:
            return True
        else:
            return False
        pass

    def get_collection_insert_date(self,collection):
        m=self.m
        m.set_collection(collection)
        df=m.read_data_to_get_field(field=['DATETIME'])
        date_list = df.DATETIME.astype(str).apply(lambda x:x[:10]).drop_duplicates().tolist()
        date_list.sort()
        return date_list
        pass

    # def multi_process_insert_min_data(stock_code_list,start_date,end_date,trade_date_list):
    #     process_list=[]
    #     sem=Semaphore(4)
    #     for stock in stock_code_list:
    #         print(stock)
    #         p=MyProcess(target=inserting_one_stock, args=(stock, start_date, end_date, trade_date_list), kwargs={'sem':sem})
    #         p.daemon=True
    #         p.start()
    #         process_list.append(p)
    #         pass
    #     for proc in process_list:
    #         proc.join()
    #         pass
    #     pass

    def single_process_insert_min_data(self,stock_code_list,trade_date_list):
        for stock in stock_code_list:
            print(stock,stock_code_list.index(stock))
            self.inserting_one_stock(stock,trade_date_list)
            pass
        pass

    def inserting_one_stock(self,stock,trade_date_list):
        start_date=trade_date_list[0]
        end_date=trade_date_list[-1]
        flag=self.check_stock_is_in_collection(stock)
        if not flag:
            self.download_and_insert(stock,start_date,end_date)
        else:
            date_list=self.get_collection_insert_date(stock[:6])
            last_insert_date=date_list[-1]
            if last_insert_date not in trade_date_list:
                return
            index=trade_date_list.index(last_insert_date)
            next_trade_date=trade_date_list[index+1]
            self.download_and_insert(stock, next_trade_date, trade_date_list[-1])
        pass

    def insert_stock_min_data(self):
        logging_joinquant()
        stock_list=get_stock_code_list()
        stock_list.sort()
        stock_list=stock_list[200:500]
        dic=get_setting_start_end_date()
        start_date=dic['start_date']
        end_date=dic['end_date']
        trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date,end_date)).astype(str)
        trade_date_list=trade_date_series.tolist()
        self.single_process_insert_min_data(stock_list,trade_date_list)
        # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
        pass

    # def insert_stock_min_data2():
    #     change_wording_address()
    #     logging_joinquant()
    #     stock_list=get_stock_code_list()
    #     stock_list.sort()
    #     stock_list=stock_list[50:100]
    #     start_date=get_setting_start_date()
    #     trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date)).astype(str)
    #     trade_date_list=trade_date_series.tolist()
    #     end_date=trade_date_list[-1]
    #     single_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    #     # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    #     pass
    #
    # def insert_stock_min_data3():
    #     change_wording_address()
    #     logging_joinquant()
    #     stock_list=get_stock_code_list()
    #     stock_list.sort()
    #     out_put_df=pd.DataFrame()
    #     for i in range(len(stock_list)//100+1):
    #         temp=pd.Series(stock_list[100*i:100*(i+1)])
    #         temp.name=i
    #         out_put_df=out_put_df.append(temp)
    #         pass
    #     out_put_df.to_csv(r'D:\code\factor_module\download_stock_min_data\stock_list.csv')
    #     stock_list=stock_list[200:300]
    #     start_date=get_setting_start_date()
    #     trade_date_series:pd.Series=pd.Series(get_trade_date_list(start_date)).astype(str)
    #     trade_date_list=trade_date_series.tolist()
    #     end_date=trade_date_list[-1]
    #     single_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    #     # multi_process_insert_min_data(stock_list,start_date,end_date,trade_date_list)
    #     pass

if __name__=='__main__':
    a=get_min_price_class()
    a.insert_stock_min_data()
    pass