# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import statsmodels.api as sm




class multi_factor_regression(object):
    def __init__(self):
        self.trade_status = None
        self.ipo = None
        self.stock_price = None
        self.index_daily_data = None
        self.circulating_market_cap = None
        self.stock_industry_code = None
        self.zz500_weight = None

        self.buy_sell_interval=5

        self.nothing=None
        pass



    def get_industry_dummy(self,stock_list, date):
        industry_data = self.stock_industry_code
        # 获取指定日期的，指定股票列表的行业字符窜
        day_industry_series = industry_data.loc[date, stock_list]
        industry_factor_df = pd.get_dummies(day_industry_series)
        industry_factor_df = industry_factor_df[industry_factor_df.sum(axis=1) == 1]
        return industry_factor_df



    # 获得行业权重
    def get_benchmark_ind_weight(self,date):
        date_timestamp=pd.to_datetime(date)
        benchmark_weight=self.zz500_weight
        industry_data = self.stock_industry_code
        benchmark_weight_series=benchmark_weight.loc[date_timestamp]
        industry_data_series = industry_data.loc[date_timestamp]
        concat_df=pd.DataFrame()
        concat_df['benchmark_ind_weight']=benchmark_weight_series
        concat_df['industry']=industry_data_series
        ind_weight=concat_df.groupby('industry').sum()/100
        for key in ind_weight.index.tolist():
            assert '8' in key
            pass
        assert (0.99<(ind_weight.sum().sum()))&((ind_weight.sum().sum())<1.01)
        return ind_weight


    # 获得因子收益率
    def get_factor_return(self,date, factor_dict):
        stock_price=self.stock_price
        circulating_market_cap=self.circulating_market_cap
        open_price=stock_price[['open','date','stock']].set_index(['date','stock']).open.unstack()
        close_price=stock_price[['close','date','stock']].set_index(['date','stock']).close.unstack()
        trade_day=open_price.index.tolist()
        std_factor_df=pd.DataFrame()
        for factor_name in factor_dict.keys():
            std_factor_df[factor_name]=factor_dict[factor_name].loc[date, :]
        ind_factor = self.get_industry_dummy(list(std_factor_df.index), date)
        # ind_factor = industry_data.loc[date]
        ind_factor.name='industry'
        factor_concat:pd.DataFrame = pd.concat([ind_factor, std_factor_df], axis=1)
        factor_concat['intercept']=1

        signal_date_idx = trade_day.index(pd.to_datetime(date))
        buy_date = trade_day[signal_date_idx+1]
        sell_date = trade_day[signal_date_idx + self.buy_sell_interval+1]


        stock_return_rate = close_price.loc[sell_date, factor_concat.index] / open_price.loc[buy_date, factor_concat.index] - 1
        factor_concat['stock_return_rate']=stock_return_rate
        stock_return_rate.name='stock_return_rate'
        benchmark_ind_weight = self.get_benchmark_ind_weight(date)

        factor_concat = factor_concat.append(benchmark_ind_weight.T)
        factor_concat.loc['benchmark_ind_weight']=factor_concat.loc['benchmark_ind_weight'].fillna(0)

        factor_concat['regress_weight'] = circulating_market_cap.loc[date,:].apply(np.sqrt)
        factor_concat.loc['benchmark_ind_weight','regress_weight']=1e20
        factor_concat.dropna(inplace=True)

        regress_x=factor_concat[factor_concat.columns.difference(['stock_return_rate','regress_weight'])]
        regress_y=factor_concat['stock_return_rate']
        regress_weight=factor_concat['regress_weight']

        model=sm.WLS(regress_y,regress_x,regress_weight)
        result=model.fit()
        return result

    def get_all_date_factor_return(self,factor_dict):
        trade_date_list=factor_dict[list(factor_dict.keys())[0]].index.tolist()[::5]
        result_dict={}
        for date in trade_date_list:
            print(date)
            result_dict[date]=self.get_factor_return(date,factor_dict)
            pass
        factor_return=pd.DataFrame()
        for date in result_dict.keys():
            temp_series=result_dict[date].params
            temp_series.name=date
            factor_return=factor_return.append(temp_series)
        return factor_return
        pass


    pass
