from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from decorate_func.decorate_function import typing_func_name

class get_zz500_weight_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.nothing=''
        pass


    def __get_zz500_weight(self, date_str):
        print(self.nothing)
        weight_df = get_index_weights('000905.XSHG', date=date_str).reset_index()
        weight_df.date=pd.to_datetime(weight_df.date)
        return weight_df
        pass

    def __logging_zz500_weight(self):
        print(self.nothing)
        # 插入数据库
        m=self.m
        # m = MongoDB_io()
        m.set_db('stock_daily_data')
        m.set_collection('zz500_weight')
        # m.insert_dataframe_to_mongodb(weight_df)
        pass

    @ typing_func_name
    def insert_zz500_weight(self,initial_flag=False):
        m=self.m
        logging_joinquant()
        self.__logging_zz500_weight()
        dic = get_setting_start_end_date()
        setting_start_date = dic['start_date']
        setting_end_date = dic['end_date']
        if initial_flag:
            trade_date_list=get_trade_date_list(setting_start_date,setting_end_date)
            pass
        else:
            _, end_date = m.get_start_end_date()
            end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')
            trade_date_list = get_trade_date_list(end_date_str,setting_end_date)

        for date in trade_date_list:
            print(date)
            df=self.__get_zz500_weight(date)
            m.insert_huge_dataframe_by_block_to_mongodb(df)
        pass

    def remove_document(self):
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('zz500_weight')
        end_date='2010-01-01'
        # df=m.read_data_to_get_dataframe_include_condition(end_date=end_date)
        # trade_date_list=df.date.drop_duplicates().tolist()
        m.delete_document_include_condition({'date':{'$lt':pd.to_datetime(end_date)}})
        pass

if __name__=='__main__':
    ## 查看数据插入到哪一天
    a=get_zz500_weight_class()
    a.insert_zz500_weight()
    pass

pass

pass