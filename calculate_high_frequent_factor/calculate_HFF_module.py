# from data_base.mongodb import MongoDB_io,cal_time
from data_base.mongodb import MongoDB_io
from multi_thread_process_module.multi_process_module import MyProcess
from multiprocessing import Semaphore
import pandas as pd
import calculate_high_frequent_factor.factor0001 as factor0001
import calculate_high_frequent_factor.factor0003 as factor0003
import calculate_high_frequent_factor.factor0004 as factor0004
import calculate_high_frequent_factor.factor0005 as factor0005
import calculate_high_frequent_factor.factor0006 as factor0006
import calculate_high_frequent_factor.factor0007 as factor0007


class HFFM(object):
    def __init__(self):
        self.start_date = '2015-01-01'
        self.end_date = '2019-09-01'
        self.m=MongoDB_io()
        self.nothing=''
        pass

    def get_trade_date_list(self):
        start_date=self.start_date
        end_date=self.end_date
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('stock_trade_date')
        trade_date_info=m.read_data_to_get_dataframe_include_condition(start_date=start_date,end_date=end_date)
        trade_date_list=trade_date_info[(trade_date_info.date > pd.to_datetime(start_date)) & (
                trade_date_info.date<pd.to_datetime(end_date))]['date'].astype(str).tolist()
        return trade_date_list

    def get_stock_list(self):
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('stock_ipo_date')
        stock_info=m.read_data_to_get_dataframe()
        stock_code_list=stock_info.stock.apply(lambda x:x[:6]).tolist()
        stock_code_list.sort()
        return stock_code_list

    @ staticmethod
    def calculate_factor(stock_code_str,start_date,end_date):
        m=MongoDB_io()
        start_date=start_date
        end_date=end_date
        m.set_MongoClient()
        m.set_db('stock_min_info')
        # 看看stock_code_str 在不在数据库里面
        if stock_code_str not in m.list_collection_names():
            return
        m.set_collection(stock_code_str)
        stock_min_data = m.read_data_from_stock_min_db(start_date,end_date)
        factor0001.calculate_up_ratio(stock_code_str,stock_min_data)
        factor0003.calculate_ocvp_bcvp(stock_min_data)
        print(stock_code_str, ' done')
        pass

    def single_process_calculate_factor(self,stock_code_list):
        start_date=self.start_date
        end_date=self.end_date
        factor_df = pd.DataFrame()
        for stock in stock_code_list:
            print(stock)
            self.calculate_factor(stock,start_date=start_date,end_date=end_date)
            pass
        return factor_df

    def multi_process_calculate_factor(self,stock_code_list):
        process_list=[]
        sem=Semaphore(4)
        start_date=self.start_date
        end_date=self.end_date
        for stock in stock_code_list:
            print(stock)
            p=MyProcess(target=self.calculate_factor,args=(stock,start_date,end_date),kwargs={'sem':sem})
            p.daemon=True
            p.start()
            process_list.append(p)
            pass
        for proc in process_list:
            proc.join()
            pass
        pass

    def run_calculation(self):
        # trade_date_list=self.get_trade_date_list()
        stock_list=self.get_stock_list()
        self.multi_process_calculate_factor(stock_list)
        # self.single_process_calculate_factor(stock_list)
        pass
    pass

if __name__=='__main__':
    h=HFFM()
    h.run_calculation()