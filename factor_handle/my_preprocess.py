import pandas as pd
import statsmodels.api as sm
from decorate_func.decorate_function import typing_func_name


#%%
class preprocess(object):
    def __init__(self):
        self.risk_factor_dict={'cap':pd.DataFrame()}
        self.stock_industry_df=None
        self.trade_status_df=None
        self.ipo_df=None
        self.nothing=''
        pass

    def de_extremum(self, factor_df):
        """
        去极值
        """
        print(self.nothing)
        middle_series:pd.Series = factor_df.quantile(0.5, axis=1)
        dm1 = abs(factor_df.sub(middle_series, axis= 0)).quantile(0.5, axis=1)
        adj_factor:pd.DataFrame = factor_df.copy()
        upper_bound = middle_series.add(5*dm1)
        lower_bound = middle_series.add(-5*dm1)
        adj_factor.mask(cond = adj_factor.gt(upper_bound, axis = 0), other = upper_bound, axis = 0,inplace = True)
        adj_factor.mask(cond = adj_factor.lt(lower_bound, axis = 0), other = lower_bound, axis = 0,inplace = True)
        return adj_factor
        pass

    def __get_industry_dummy(self, stock_list, date):
        """
        # 获得行业哑变量
        :param stock_list:
        :param date:
        :return:
        """
        industry_data=self.stock_industry_df
        #获取指定日期的，指定股票列表的行业字符窜
        day_industry_series = industry_data.loc[date,stock_list]
        industry_factor_df = pd.get_dummies(day_industry_series)
        industry_factor_df=industry_factor_df[industry_factor_df.sum(axis=1)==1]
        return industry_factor_df

    @ typing_func_name
    def neutralize(self, un_neutralize_factor):
        """
        # 中性化因子
        """
        risk_factor_dict=self.risk_factor_dict
        trade_status_df=self.trade_status_df
        ipo_days=self.ipo_df

        adjusted_alpha_df=pd.DataFrame()
        for date in un_neutralize_factor.index:
            print(date)
            alpha_data_series = un_neutralize_factor.loc[date, :]
            alpha_data_series.name='alpha'
            risk_factor_df=pd.DataFrame()
            for risk_factor in risk_factor_dict.keys():
                risk_factor_series=risk_factor_dict[risk_factor].loc[date]
                risk_factor_series.name=risk_factor
                risk_factor_df = risk_factor_df.append(risk_factor_series)
                pass
            risk_factor_df=risk_factor_df.T
            ind_factor_df = self.__get_industry_dummy(alpha_data_series.index, date)
            # 过滤上市时间不超过100天的，交易状态为不可交易的股票
            listed_series = ipo_days.loc[date]
            listed_day_list = listed_series[listed_series>100].index.tolist()
            trade_status_series = trade_status_df.loc[date]
            trade_status_list = trade_status_series[trade_status_series==1].index.tolist()
            target_stock_list = list(set(listed_day_list)&set(trade_status_list))
            # 合成一个包含行业，风格（市值），和待中性化的因子的矩阵。
            concat_df:pd.DataFrame=pd.concat([ind_factor_df,risk_factor_df,alpha_data_series],axis = 1)
            factor_df:pd.DataFrame = concat_df.loc[target_stock_list]
            factor_df.dropna(inplace=True)
            factor_df.sort_index(inplace=True)
            factor_df['intercept']=1
            alpha_name = alpha_data_series.name
            ind_name = ind_factor_df.columns.tolist()
            style_name = risk_factor_df.columns.tolist()
            x = factor_df[['intercept']+ind_name + style_name]
            y = factor_df[alpha_name]
            model = sm.OLS(y,x)
            result = model.fit()
            residual = result.resid
            residual.name=date
            adjusted_alpha_df = adjusted_alpha_df.append(residual)
            pass
        return adjusted_alpha_df
        pass

    def standardize(self,factor_df):
        print(self.nothing)
        std_factor = (factor_df.sub(factor_df.mean(axis=1), axis=0)).divide(factor_df.std(axis=1), axis=0)
        return std_factor
        pass



    # def get_preprocess_factor(self,factor_df):
    #     """
    #     这个适用于算好因子，中性化后送去分层回测。
    #     :return:
    #     """
    #     de_extremum_factor=self.de_extremum(factor_df)
    #     neutralize_factor=self.neutralize(de_extremum_factor)
    #     standardize_factor=self.standardize(neutralize_factor)
    #     return standardize_factor
    #     pass

    # @ print_func_name
    # def get_neutralized_factor(self,factor_df):
    #     """
    #     中性化因子
    #     :return:
    #     """
    #     return self.neutralize(factor_df)
    #     pass

    def get_neutralize_z_score_data(self, factor_df):
        de_extremum_factor=self.de_extremum(factor_df)
        neutralize_factor=self.neutralize(de_extremum_factor)
        standardize_factor=self.standardize(neutralize_factor)
        return standardize_factor
        pass

    pass

# class MFP(preprocess):
#     def __init__(self):
#         super(MFP,self).__init__()
#         self.stock_500_component=None
#         pass
#
#     @ print_func_name
#     def standardize_by_benchmark(self,factor_df):
#         """
#         根据基准标准化
#         :return:
#         """
#         # bench_mark_weight 按日期加起来是小于1
#         bench_mark_weight=self.stock_500_component.loc[factor_df.index,factor_df.columns].copy()
#         weight_sum = bench_mark_weight[factor_df.notnull()].sum(axis=1)
#         bench_mark_weight_new = bench_mark_weight.div(weight_sum, axis=0)
#         benchmark_weighted_mean = bench_mark_weight_new.mul(factor_df, axis=1).sum(axis=1)
#         std_factor = factor_df.sub(benchmark_weighted_mean, axis=0).div(factor_df.std(axis=1), axis=0)
#         return std_factor
#         pass
#
#     @ print_func_name
#     def MFPF(self,factor_df):
#         """
#         这个适用于算好因子，中性化后送去分层回测。
#         :return:
#         """
#         de_extremum_factor=self.de_extremum(factor_df)
#         neutralize_factor=self.neutralize(de_extremum_factor)
#         standardize_factor=self.standardize_by_benchmark(neutralize_factor)
#         return standardize_factor
#         pass
#
#     pass





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


