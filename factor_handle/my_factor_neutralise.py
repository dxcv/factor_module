import pandas as pd
import numpy as np
import statsmodels.api as sm
# import time
import sys
sys.path.append('D:/code/tick_data_handle/read_mongodb/')
# from data_base import MongoDB_io

#%%
class preprocess(object):
    def __init__(self):
        # read_data
        path='D:/code/tick_data_handle/prepare_neutralise_data/prepare_data/'
        day_data_dict=pd.read_pickle(path+'stock_daily_market_data.pkl').to_dict()
        self.day_ipo=pd.read_pickle(path+'ipo_day.pkl')
        self.barra_dict = {'lncap':pd.read_pickle(path+'capital.pkl')}
        self.stock_500_component = pd.read_pickle(path+'zz500_weight.pkl')

        # 改格式
        for factor in day_data_dict.keys():
            print(factor)
            day_data_dict[factor].columns=day_data_dict[factor].columns.to_series(

            ).apply(lambda x:x[:6]+'.XSHE' if x[-2:]=='SZ' else x[:6]+'.XSHG')
            pass

        #
        self.day_data_dict=day_data_dict

        #
        self.org_factor=pd.DataFrame()
        self.filled_factor=pd.DataFrame()
        self.winsorized_factor=pd.DataFrame()
        self.neutralized_factor=pd.DataFrame()
        self.std_factor=pd.DataFrame()
        pass

    def set_factor_to_handle(self,df):
        self.org_factor=df
        pass

    def fill_na_with_ind_mean(self):
        """
        填写行业均值
        """
        factor_df:pd.DataFrame=self.org_factor.copy()
        day_data_dict = self.day_data_dict
        day_ipo_df = self.day_ipo
        try:
            factor_df.mask(cond=np.isinf(factor_df),inplace=True)
        except BaseException as e:
            print(e)
        ind_df = day_data_dict['industry'].loc[factor_df.index,factor_df.columns]
        status_df = day_data_dict['trade_status'].loc[factor_df.index,factor_df.columns]
        ind_mean_df = pd.DataFrame()
        for date in factor_df.index:
            print(date)
            daily_factor = factor_df.loc[date]
            daily_ipo = day_ipo_df.loc[date]
            daily_ind:pd.Series = ind_df.loc[date]
            daily_status = status_df.loc[date]
            # 过滤不合条件的股票
            avl_stock = daily_ipo[(daily_ipo >60) & (daily_ind.isnull() == 0) & (daily_status == 1)].index.tolist()
            daily_factor = daily_factor.loc[avl_stock]
            daily_ind = daily_ind.loc[avl_stock]
            daily_df:pd.DataFrame = pd.concat([daily_factor,daily_ind],axis=1)
            daily_df.columns = ['factor','ind']
            mean_daily_df = daily_df[(daily_df['factor']<daily_df['factor'].quantile(0.99)) & (daily_df['factor']>daily_df['factor'].quantile(0.01))]
            ind_mean = mean_daily_df.groupby('ind')['factor'].mean()
            ind_mean.name=date
            ind_mean_df=ind_mean_df.append(ind_mean)
            pass

        filled_factor_df:pd.DataFrame = factor_df.copy()
        ind_mean_df.mask(cond = ind_mean_df.isnull(), other = factor_df.mean(axis=1), inplace = True, axis = 0)
        for ind in ind_mean_df.columns:
            print(ind)
            filled_factor_df.mask(cond = (filled_factor_df.isnull()) & (ind_df == ind), other = ind_mean_df[ind],inplace = True, axis  = 0)
            pass
        # self.filled_factor_df=filled_factor_df
        self.filled_factor=filled_factor_df
        pass

    def winsorize(self):
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

    def winsorize_by_benchmark(self):
        """
        去极值，过滤不符合条件的股票
        """
        factor_df:pd.DataFrame=self.filled_factor.copy()
        day_status = self.day_data_dict['trade_status']
        day_st = self.day_data_dict['is_st']
        ipo_day = self.day_ipo

        date_range = factor_df.index
        cond_1 = ipo_day.loc[date_range] > 90
        cond_2 = day_status.loc[date_range] == 1
        cond_3 = day_st.loc[date_range] == 0
        avl_cond = cond_1 & cond_2 & cond_3
        avl_cond.dropna(how='all', inplace=True, axis=1)
        factor_df = factor_df.loc[avl_cond.index, avl_cond.columns]
        filter_factor_df = factor_df[avl_cond]

        middle_series = filter_factor_df.quantile(0.5, axis=1)
        dm1 = abs(filter_factor_df.sub(middle_series, axis=0)).quantile(0.5, axis=1)
        adj_factor: pd.DataFrame = filter_factor_df.copy()
        upper_bound = middle_series.add(5 * dm1)
        lower_bound = middle_series.add(-5 * dm1)
        adj_factor.mask(cond=adj_factor.gt(upper_bound, axis=0), other=upper_bound, axis=0, inplace=True)
        adj_factor.mask(cond=adj_factor.lt(lower_bound, axis=0), other=lower_bound, axis=0, inplace=True)
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

    def standardize_by_benchmark(self):
        """
        根据基准标准化
        :return:
        """
        factor_df:pd.DataFrame=self.neutralized_factor.copy()
        # bench_mark_weight 按日期加起来是小于1
        bench_mark_weight=self.stock_500_component.loc[factor_df.index,factor_df.columns].copy()
        weight_sum = bench_mark_weight[factor_df.notnull()].sum(axis=1)
        bench_mark_weight_new = bench_mark_weight.div(weight_sum, axis=0)
        benchmark_weighted_mean = bench_mark_weight_new.mul(factor_df, axis=1).sum(axis=1)
        std_factor = factor_df.sub(benchmark_weighted_mean, axis=0).div(factor_df.std(axis=1), axis=0)
        # temp=std_factor.mul(bench_mark_weight)
        # temp.sum(axis=1)
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


    # def neutralize(self):
    #     """
    #     # 中性化因子
    #     """
    #     sty_factor_dict=self.barra_dict
    #     test_factor_df:pd.DataFrame=self.winsorized_factor
    #     day_status=self.day_data_dict['trade_status']
    #     listed_days=self.day_ipo
    #     adjusted_alpha_df=pd.DataFrame()
    #     for date in test_factor_df.index:
    #         print(date)
    #         alpha_data_series = test_factor_df.loc[date, :]
    #         alpha_data_series.name='alpha'
    #         style_factor_df=pd.DataFrame()
    #         for style_factor in sty_factor_dict.keys():
    #             style_factor_series=sty_factor_dict[style_factor].loc[date]
    #             style_factor_series.name=style_factor
    #             style_factor_df = style_factor_df.append(style_factor_series)
    #             pass
    #         style_factor_df=style_factor_df.T
    #         ind_factor_df = self.get_industry_dummy(alpha_data_series.index, date)
    #         # 过滤上市时间不超过100天的，交易状态为不可交易的股票
    #         listed_series = listed_days.loc[date]
    #         listed_day_list = listed_series[listed_series>100].index.tolist()
    #         trade_status_series = day_status.loc[date]
    #         trade_status_list = trade_status_series[trade_status_series==1].index.tolist()
    #         target_stock_list = list(set(listed_day_list)&set(trade_status_list))
    #         # 合成一个包含行业，风格（市值），和待中性化的因子的矩阵。
    #         concat_df:pd.DataFrame=pd.concat([ind_factor_df,style_factor_df,alpha_data_series],axis = 1)
    #         factor_df:pd.DataFrame = concat_df.loc[target_stock_list]
    #         factor_df.dropna(inplace=True)
    #         factor_df.sort_index(inplace=True)
    #         factor_df['intercept']=1
    #         alpha_name = alpha_data_series.name
    #         ind_name = ind_factor_df.columns.tolist()
    #         style_name = style_factor_df.columns.tolist()
    #         x = factor_df[['intercept']+ind_name + style_name]
    #         y = factor_df[alpha_name]
    #         model = sm.OLS(y,x)
    #         result = model.fit()
    #         residual = result.resid
    #         residual.name=date
    #         adjusted_alpha_df = adjusted_alpha_df.append(residual)
    #         pass
    #     self.neutralized_factor=adjusted_alpha_df
    #     pass

    def get_preprocess_factor(self):
        """
        这个适用于算好因子，中性化后送去分层回测。
        :return:
        """
        self.fill_na_with_ind_mean()
        self.winsorize()
        self.neutralize()
        self.standardize()
        return self.std_factor
        pass

    def get_preprocess_factor_v2(self):
        """
        这个适用于回归，算因子收益率的时候。
        :return:
        """
        self.fill_na_with_ind_mean()
        # self.winsorize_by_benchmark()
        self.winsorize()
        self.neutralize()
        self.standardize_by_benchmark()
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


