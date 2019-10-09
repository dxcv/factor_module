from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from decorate_func.decorate_function import typing_func_name

class get_capital_data_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.nothing=''
        pass


    def __get_one_day_capital(self,date_str):
        print(self.nothing)
        q = query(valuation).filter(valuation.market_cap > 0)
        capital_df = get_fundamentals(q,date_str)
        capital_df=capital_df.loc[:,['code','day','market_cap','circulating_cap','circulating_market_cap']]
        capital_df.rename(columns={'day':'date'},inplace=True)
        return capital_df

    def __logging_capital_data_db(self):
        m=self.m
        # 插入数据库
        # m = MongoDB_io()
        m.set_db('stock_daily_data')
        m.set_collection('stock_capital_data')
        pass

    @ typing_func_name
    def insert_capital_data(self,initial_flag=False):
        m=self.m
        logging_joinquant()
        self.__logging_capital_data_db()
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
            capital=self.__get_one_day_capital(date)
            m.insert_dataframe_to_mongodb(capital)
            pass
        m.close_MongoDB_connection()
        pass

    def remove_document(self):
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('stock_capital_data')
        end_date='2010-01-01'
        # df=m.read_data_to_get_dataframe_include_condition(end_date=end_date)
        # trade_date_list=df.date.drop_duplicates().tolist()
        m.delete_document_include_condition({'date':{'$lt':pd.to_datetime(end_date)}})
        pass



if __name__=='__main__':
    a=get_capital_data_class()
    a.insert_capital_data()
    pass