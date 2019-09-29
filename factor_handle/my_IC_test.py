import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import spearmanr

class IC_test_module(object):
    def __init__(self):
        self.nothing=None
        self.stock_daily_price=None
        self.trade_status=None
        self.stock_ipo = None
        pass

    def Rank_Correlation(self,factor_df, return_df, significant = 0.05):
        print(self.nothing)
        #创建空列表
        corr=pd.Series([],name='corr')
        p_values=pd.Series([],name='p_values')

        #每月计算因子与收益率的相关系数
        for date in factor_df.index:
            #将每月的因子和次月的收益率合并为dataframe
            factor_return_df=pd.DataFrame({'x':factor_df.loc[date], 'y':return_df.loc[date]})
            factor_return_df.dropna(how='any',inplace=True)
            #计算每月的秩相关系数
            spear = spearmanr(factor_return_df.x,factor_return_df.y)
            corr[date]=spear[0]#将相关系数插入列表
            p_values[date]=spear[1]#将pvalues插入列表
            pass

        #以下操作与稳健回归函数类似
        all_corr:pd.DataFrame = pd.concat([corr,p_values],axis=1)
        mean_ic = all_corr['corr'].mean()
        std_ic = all_corr['corr'].std()
        ir = (mean_ic*250)/(std_ic*np.sqrt(250))
        ## 相关性显著比例
        corr_pos_sig = all_corr[(all_corr['corr']>0) & (all_corr['p_values']<significant)].shape[0]/(all_corr.shape[0]*1.0)
        corr_neg_sig = all_corr[(all_corr['corr']<=0) & (all_corr['p_values']<significant)].shape[0]/(all_corr.shape[0]*1.0)

        result_dict=dict()
        result_dict['ic_stat'] = pd.Series({ 'annualised ir':ir,'mean ic':mean_ic, 'std ic':std_ic,'corr_pos_sig':corr_pos_sig,'corr_neg_sig':corr_neg_sig})
        result_dict['corr']=all_corr

        return result_dict


    def IC_test(self,factor_df, holding_period=1):
        stock_price=self.stock_daily_price
        trade_status = self.trade_status
        trade_day_list=stock_price['date'].drop_duplicates().tolist()
        stock_ipo=self.stock_ipo

        close_back_adj = stock_price[['close','date','stock']].set_index(['date','stock']).close.unstack()
        open_back_adj = stock_price[['open','date','stock']].set_index(['date','stock']).open.unstack()

        signal_date = [factor_df.index[i] for i in range(0, len(factor_df.index), holding_period)]
        # 买股票需要滞后一天
        buy_date = [trade_day_list[trade_day_list.index(i)+1] for i in signal_date]
        sell_date = [trade_day_list[trade_day_list.index(i)+holding_period] for i in signal_date]

        overnight_return = open_back_adj / close_back_adj.shift(1) - 1
        overnight_return=overnight_return.loc[signal_date]

        buy_status_cond = trade_status.loc[buy_date]==True
        sell_status_cond = trade_status.loc[sell_date]==True
        buy_status_cond.index = signal_date
        sell_status_cond.index = signal_date
        overnight_cond = overnight_return<=0.09
        ipo_cond = stock_ipo.loc[signal_date]>60
        all_cond = (buy_status_cond & sell_status_cond & overnight_cond & ipo_cond)
        # all_cond = (buy_status_cond & sell_status_cond & overnight_cond )

        buy_date_open = open_back_adj.loc[buy_date]
        sell_date_close = close_back_adj.loc[sell_date]
        buy_date_open.index =signal_date
        sell_date_close.index = signal_date

        signal_df = (factor_df.loc[signal_date]).loc[all_cond.index,all_cond.columns][all_cond==True]
        holding_rate = (sell_date_close / buy_date_open)-1
        result_dict = self.Rank_Correlation(signal_df, holding_rate, significant = 0.05)
        return result_dict



    def draw_ic_plot(self,result_dict):
        print(self.nothing,)
        all_corr=result_dict['corr']
        corr_significant = all_corr['corr'][all_corr['p_values']<0.05]
        corr_non_significant = all_corr['corr'][all_corr['p_values']>=0.05]
        corr_graph = pd.DataFrame({'corr_significant':corr_significant,'corr_non_significant':corr_non_significant})
        x_axis = pd.to_datetime(corr_graph.index)
        fig = plt.figure()
        fig.set_size_inches(15, 10)
        ax1 = fig.add_subplot(111)
        ax1.bar(x_axis,corr_graph['corr_significant'],width = 1,color = 'green',label='Significant IC')
        ax1.bar(x_axis,corr_graph['corr_non_significant'],width = 1,color = 'red',label='Non_Significant IC')
        ax1.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        plt.show()
        return fig
        pass

    def factor_rolling_mean_ic_test(self, factor_df, mean_window_len = 1):
        period_list = [1,3,5,10]
        ic_info= pd.DataFrame()
        for period in period_list:
            mean_factor_df = factor_df.rolling(mean_window_len, min_periods = int(round(0.8 * mean_window_len, 0))).mean()
            mean_factor_df.dropna(how='all',inplace=True)
            result_dict = self.IC_test(mean_factor_df,period)
            result_series=pd.Series(result_dict['ic_stat'],name=str(period))
            ic_info=ic_info.append(result_series)
            pass
        print(self.nothing)
        return ic_info

    def different_rolling_length_ic_test(self,factor_df):
        # result_dict=self.IC_test(factor_df)
        # self.draw_ic_plot(result_dict)
        ic_info_concat=pd.DataFrame()
        for i in range(1,6):
            ic_info=self.factor_rolling_mean_ic_test(factor_df)
            ic_info.index=pd.MultiIndex.from_product([['Ma'+str(i)],ic_info.index.tolist()])
            ic_info_concat=ic_info_concat.append(ic_info)
            pass
        return ic_info_concat
        pass

    pass