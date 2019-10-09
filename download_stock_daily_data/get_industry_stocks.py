from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *
import pandas as pd
from decorate_func.decorate_function import typing_func_name


class get_industry_stock_class(object):
    def __init__(self):
        self.m=MongoDB_io()
        self.nothing=''
        pass

    def get_sw_industry_level1_list(self):
        print(self.nothing)
        return get_industries(name='sw_l1')

    def get_one_date_industry_data(self,date_str,industry_code_list):
        print(self.nothing)
        daily_industry_series = pd.Series()
        for industry_code in industry_code_list.index[:]:
            stock_list = get_industry_stocks(industry_code, date=date_str)
            daily_industry_series = daily_industry_series.append(pd.Series(industry_code, index=stock_list))
            pass
        daily_industry_series.index=pd.MultiIndex.from_product([[date_str],daily_industry_series.index])
        daily_industry_df=daily_industry_series.to_frame().reset_index()
        daily_industry_df.columns=['date','stock','industry_category']
        return daily_industry_df
        pass

    def logging_industry_stocks(self):
        m=self.m
        m.set_db('stock_daily_data')
        m.set_collection('stock_sw_industry_code')
        pass

    @ typing_func_name
    def insert_industry_stock(self,initial_flag=False):
        m=self.m
        logging_joinquant()
        self.logging_industry_stocks()
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
        industry_list=self.get_sw_industry_level1_list()
        for date in trade_date_list:
            print(date)
            one_day_data=self.get_one_date_industry_data(date,industry_list)
            m.insert_dataframe_to_mongodb(one_day_data)
            pass
        m.close_MongoDB_connection()
        pass

    # def delete_duplicate_document(self):
    #     m=self.m
    #     self.logging_industry_stocks()
    #     df=m.read_data_to_get_dataframe_include_condition(start_date='2010-01-01',end_date='2010-01-06')
    #     ## 发现重复的日期是2010-01-04， 2010-01-05
    #     condition1 = dict()
    #     condition2 = dict()
    #
    #     condition1['date'] = pd.to_datetime('2010-01-04')
    #     condition2['date'] = pd.to_datetime('2010-01-05')
    #     condition = {'$or': [condition1, condition2]}
    #
    #     feedback=m.delete_document_include_condition(condition)
    #     df_processed=df.drop_duplicates()
    #     m.insert_dataframe_to_mongodb(df_processed)
    #     print(feedback)
    #     pass


if __name__=='__main__':
    a=get_industry_stock_class()
    a.insert_industry_stock()
    pass

