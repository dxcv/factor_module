from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from decorate_func.decorate_function import typing_func_name

class get_market_price_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.nothing=''
        pass

    def __get_security_from_joinquant(self):
        print(self.nothing)
        df = get_all_securities(types=[], date=None)
        return df.index.tolist()
        pass

    def __get_stock_daily_price(self, date, stock_list, pre_flag=None):
        print(self.nothing)
        if pre_flag=='pre':
            panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                              fq='pre', count=None)
        elif pre_flag=='post':
            panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                              fq='post', count=None)
        else:
            panel = get_price(stock_list, start_date=date, end_date=date, frequency='daily', fields=None, skip_paused=False,
                              fq=None, count=None)
            pass
        df=panel.iloc[:,0,:]
        df.reset_index(inplace=True)
        df.rename(columns={'index':'stock'},inplace=True)
        df['date']=pd.to_datetime(date)
        return df
        pass

    def __logging_market_data_db(self, pre_flag=None):
        m=self.m
        m.set_db('stock_daily_data')
        if pre_flag=='pre':
            m.set_collection('stock_pre_price')
        elif pre_flag=='post':
            m.set_collection('stock_post_price')
        else:
            m.set_collection('stock_real_price')
        # m.insert_dataframe_to_mongodb(df)
        pass

    def __insert_price_data(self, initial_flag=False, pre_flag_ = None):
        m = self.m
        logging_joinquant()
        self.__logging_market_data_db(pre_flag=pre_flag_)
        stock_code_list=self.__get_security_from_joinquant()
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

        for date_str in trade_date_list:
            print(date_str)
            price_df=self.__get_stock_daily_price(date_str, stock_code_list, pre_flag=pre_flag_)
            price_df.dropna(inplace=True)
            m.insert_dataframe_to_mongodb(price_df)
            pass
        pass

    def drop_duplicate_document(self,fq=None):
        m=self.m
        if fq is None:
            collection_name='stock_real_price'
        elif fq=='post':
            collection_name='stock_post_price'
        else:
            return
        m.set_db('stock_daily_data')
        logging_joinquant()
        dic = get_setting_start_end_date()
        setting_start_date = dic['start_date']
        setting_end_date = dic['end_date']
        trade_date_list = get_trade_date_list(setting_start_date, setting_end_date)
        for date in trade_date_list:
            date_str=str(date)
            print(date_str)
            m.set_collection(collection_name)
            df=m.read_data_to_get_dataframe_in_one_date(date_str)
            df_unique=df.dropna()
            m.set_collection(collection_name+'_')
            m.insert_dataframe_to_mongodb(df_unique)
            pass
        pass

    @ typing_func_name
    def insert_real_price_data(self,initial_flag=False):
        self.__insert_price_data(initial_flag=initial_flag, pre_flag_ = None)
        pass

    @ typing_func_name
    def insert_post_price_data(self,initial_flag=False):
        self.__insert_price_data(initial_flag=initial_flag, pre_flag_ ='post')
        pass

if __name__=='__main__':
    # insert_price_data()
    a=get_market_price_class()
    a.drop_duplicate_document()
    pass