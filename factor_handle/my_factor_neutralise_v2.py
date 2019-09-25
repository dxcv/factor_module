import pandas as pd


#%%
class preprocess(object):
    def __init__(self):
        self.unprocessed_factor_df=None
        self.capital_df=None
        self.stock_industry_df=None
        self.status_df=None


        # self.filled_factor=pd.DataFrame()
        self.winsorized_factor=pd.DataFrame()
        self.neutralized_factor=pd.DataFrame()
        self.std_factor=pd.DataFrame()
        pass


    def de_extremum(self):
        """
        去极值
        """
        # factor_df:pd.DataFrame = self.filled_factor_df
        factor_df:pd.DataFrame=self.filled_factor.copy()
        middle_series:pd.Series = factor_df.quantile(0.5, axis=1)
        dm1 = abs(factor_df.sub(middle_series, axis= 0)).quantile(0.5, axis=1)
        adj_factor:pd.DataFrame = factor_df.copy()
        upper_bound = middle_series.add(5*dm1)
        lower_bound = middle_series.add(-5*dm1)
        adj_factor.mask(cond = adj_factor.gt(upper_bound, axis = 0), other = upper_bound, axis = 0,inplace = True)
        adj_factor.mask(cond = adj_factor.lt(lower_bound, axis = 0), other = lower_bound, axis = 0,inplace = True)
        # std_factor = (adj_factor.sub(adj_factor.mean(axis=1), axis=0)).divide(adj_factor.std(axis=1), axis=0)
        # self.std_factor=std_factor
        self.winsorized_factor=adj_factor
        pass




    def standardize(self):
        """
        标准化
        """
        factor_df:pd.DataFrame=self.neutralized_factor.copy()
        std_factor = (factor_df.sub(factor_df.mean(axis=1), axis=0)).divide(factor_df.std(axis=1), axis=0)
        self.std_factor=std_factor
        pass




    def get_industry_dummy(self, stock_list, date):
        """
        # 获得行业哑变量
        :param stock_list:
        :param date:
        :return:
        """
        industry_data=self.day_data_dict['industry']
        #获取指定日期的，指定股票列表的行业字符窜
        day_industry_series = industry_data.loc[date,stock_list]
        industry_factor_df = pd.get_dummies(day_industry_series)
        industry_factor_df=industry_factor_df[industry_factor_df.sum(axis=1)==1]
        return industry_factor_df


    def neutralize(self):
        """
        # 中性化因子
        """
        sty_factor_dict=self.barra_dict
        test_factor_df:pd.DataFrame=self.winsorized_factor
        day_status=self.day_data_dict['trade_status']
        listed_days=self.day_ipo
        adjusted_alpha_df=pd.DataFrame()
        for date in test_factor_df.index:
            print(date)
            alpha_data_series = test_factor_df.loc[date, :]
            alpha_data_series.name='alpha'
            style_factor_df=pd.DataFrame()
            for style_factor in sty_factor_dict.keys():
                style_factor_series=sty_factor_dict[style_factor].loc[date]
                style_factor_series.name=style_factor
                style_factor_df = style_factor_df.append(style_factor_series)
                pass
            style_factor_df=style_factor_df.T
            ind_factor_df = self.get_industry_dummy(alpha_data_series.index, date)
            # 过滤上市时间不超过100天的，交易状态为不可交易的股票
            listed_series = listed_days.loc[date]
            listed_day_list = listed_series[listed_series>100].index.tolist()
            trade_status_series = day_status.loc[date]
            trade_status_list = trade_status_series[trade_status_series==1].index.tolist()
            target_stock_list = list(set(listed_day_list)&set(trade_status_list))
            # 合成一个包含行业，风格（市值），和待中性化的因子的矩阵。
            concat_df:pd.DataFrame=pd.concat([ind_factor_df,style_factor_df,alpha_data_series],axis = 1)
            factor_df:pd.DataFrame = concat_df.loc[target_stock_list]
            factor_df.dropna(inplace=True)
            factor_df.sort_index(inplace=True)
            factor_df['intercept']=1
            alpha_name = alpha_data_series.name
            ind_name = ind_factor_df.columns.tolist()
            style_name = style_factor_df.columns.tolist()
            x = factor_df[['intercept']+ind_name + style_name]
            y = factor_df[alpha_name]
            model = sm.OLS(y,x)
            result = model.fit()
            residual = result.resid
            residual.name=date
            adjusted_alpha_df = adjusted_alpha_df.append(residual)
            pass
        self.neutralized_factor=adjusted_alpha_df
        pass

    def get_preprocess_factor(self):
        """
        这个适用于算好因子，中性化后送去分层回测。
        :return:
        """
        # self.fill_na_with_ind_mean()
        self.de_extremum()
        self.neutralize()
        self.standardize()
        return self.std_factor
        pass


    pass







#%%


#
# if __name__=='__main__':
#     factor_data_MIDF=pd.read_pickle('D:/code/tick_data_handle/hdf_to_factor/force_compare_ratio.pkl')
#     factor_data_df=factor_data_MIDF.iloc[:,1].unstack().T.iloc[0:1]
#     p=preprocess()
#     p.set_factor_to_handle(factor_data_df)
#     std_df=p.get_preprocess_factor_v2()
#     std_df.to_pickle('D:/code/tick_data_handle/neutralized_factor_files/'+'n_factor.pkl')
#     print()


