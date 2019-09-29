# import scipy.stats as sps
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from factor_handle.my_back_test_engine import back_test_engine
from factor_handle.my_IC_test import IC_test_module
import collections
from factor_handle.docx_module import doc_class

#%%
class factor_test(back_test_engine,IC_test_module):
    def __init__(self):
        super(factor_test,self).__init__()
        self.stock_daily_price = None
        self.stock_ipo = None
        self.index_daily_data = None
        self.trade_status=None
        self.nothing= ''
        self.group_num=5
        pass


    def get_stock_weight_by_lay(self, factor_df:pd.DataFrame, holding_period:int):
        """
        :param factor_df:
        :param holding_period:
        :return:
        """
        group_num=self.group_num
        trade_status=self.trade_status

        # 获得需要因子分层的日期,signal_date_list为因子分组的日期
        trade_date_list = trade_status.index.tolist()
        trade_date_list.sort()
        trade_date_1day_lag = dict(zip(trade_date_list[:-1], trade_date_list[1:]))
        signal_date_list = factor_df[factor_df.index.isin(holding_period)].index.tolist()

        # 生成容器（dict），存放每个分组的权重dataframe,字典key：分组号，value：分组对应的股票权重。
        group_stock_weight_dict = {}
        for group_name in ['group_' + str(x) for x in range(group_num)]:
            group_stock_weight_dict[group_name] = pd.DataFrame()

        for date in signal_date_list:
            daily_factor_series = factor_df.loc[date]
            daily_status_series = trade_status.loc[date]
            daily_factor_series = daily_factor_series[daily_status_series[daily_status_series == True].index]
            daily_factor_series.dropna(inplace=True)
            daily_factor_series.sort_values(inplace=True)
            group_stock_num = int(len(daily_factor_series) / group_num)
            for group_name in group_stock_weight_dict.keys():
                group = int(group_name.strip('group_'))
                if group == group_num - 1:
                    daily_group_stock_list = daily_factor_series.iloc[group * group_stock_num:].index.tolist()
                else:
                    daily_group_stock_list = daily_factor_series.iloc[
                                             group * group_stock_num: (group + 1) * group_stock_num].index.tolist()
                ## 因子分组要将日期延后一天，形成交易
                daily_group_holding = pd.Series(1.0 / len(daily_group_stock_list), index=daily_group_stock_list,
                                                name=trade_date_1day_lag[date])
                group_stock_weight_dict[group_name] = group_stock_weight_dict[group_name].append(daily_group_holding)
                pass
            pass

        return group_stock_weight_dict


    def back_test_by_lay(self,group_stock_weight_dict):
        # 分层回测
        # group0是因子值最小组
        group_account_dict=collections.OrderedDict()
        for group_name in group_stock_weight_dict.keys():
            print(group_name)
            stock_weight = group_stock_weight_dict[group_name]
            group_account_dict[group_name] = self.according_weight_back_test(stock_weight)
            pass
        return group_account_dict

    def draw_net_value(self,group_account_dict):
        # draw plot
        index_net_value=self.index_net_value
        fig = plt.figure()
        fig.set_size_inches(12,21)
        ax1 = fig.add_subplot(311)
        ax2 = fig.add_subplot(312)
        ax3 = fig.add_subplot(313)
        for group in group_account_dict:
            ax1.plot(group_account_dict[group]['nav_evening'].index, group_account_dict[group]['nav_evening'].values, label=group)
            hedge_nav=(group_account_dict[group]['nav_evening']-index_net_value).dropna()
            ax2.plot(hedge_nav.index, hedge_nav.values, label=group)
            pass
        group_0=list(group_account_dict.keys())[0]
        group_n=list(group_account_dict.keys())[-1]
        ax3.plot(group_account_dict[group_0]['nav_evening'].index, (group_account_dict[group_0]['nav_evening']-
                                                                  group_account_dict[group_n]['nav_evening']).values, label=group_0+'-'+group_n)
        ax1.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        ax2.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        plt.show()
        return fig
        pass

    def get_index_net_value(self, date_list):
        index_data = self.index_daily_data
        st = date_list[0]
        ed = date_list[-1]
        index_data_ = index_data.loc[st:ed]
        index_net_value = index_data_.close / index_data_.open[0]
        self.index_net_value = index_net_value
        pass

    def get_all_group_performance(self,group_account_dict):
        group_performance_df=pd.DataFrame()
        for group_name in group_account_dict.keys():
            account = group_account_dict[group_name]
            performance_dict = self.get_portfolio_performance_rate(account['nav_evening'],account['turnover'])
            group_performance_df=group_performance_df.append(pd.Series(performance_dict,name=group_name))
            pass
        return group_performance_df
        pass


    def get_portfolio_performance_rate(self, day_capital_series, turnover_series):
        """
        根据净值曲线计算 夏普等比率
        :param day_capital_series:
        :param turnover_series:
        :return:
        """
        print(self.nothing)
        def sharp_ratio(day_rate_series, period='day', re=0.02):
            if period == 'day':
                period_interval = 245
            elif period == 'month':
                period_interval = 12
            else:
                period_interval = 1
            day_rate_series[np.isnan(day_rate_series)] = 0
            annual_return_rate = np.prod(day_rate_series + 1) ** (1 / (len(day_rate_series) / period_interval)) - 1
            yearly_volatility = np.sqrt(period_interval) * np.std(day_rate_series)
            return (annual_return_rate - re) / yearly_volatility

        day_capital_df = day_capital_series.reset_index()
        day_capital_df.columns = ['date', 'rate']
        day_capital_df['month'] = day_capital_df.date.astype(str).apply(lambda x: x[:7])
        month_capital_series = day_capital_df.groupby('month').apply(lambda x: x.rate.iloc[-1] - x.rate.iloc[0])
        # 计算最大回撤
        draw_down = []
        for date in day_capital_series.index:
            drop = 1 - day_capital_series[date] / day_capital_series[:date].max()
            draw_down.append(drop)
            pass

        max_drop = max(draw_down)

        # 计算夏普比率
        day_rate = (day_capital_series / day_capital_series.shift(1) - 1)[1:]
        sharp = sharp_ratio(day_rate, period='day')

        # 计算算年化收益率
        annual_rate = (day_capital_series[-1] / day_capital_series[0]) ** (1 / (len(day_capital_series) / 245)) - 1

        # 计算日胜率
        day_win_rate = len(day_rate[day_rate > 0]) / len(day_rate)
        # 计算月胜率
        month_win_rate = len(month_capital_series[month_capital_series > 0]) / len(month_capital_series)
        # 收益波动率
        volatility = np.sqrt(245) * np.std(day_rate)
        # 平均换手率
        mean_turnover = turnover_series.mean()

        back_test_ratio = {'Annualized Return': round(annual_rate, 4),
                           'Max Retraction': round(max_drop, 4),
                           'Sharp Ratio': round(sharp, 4),
                           'Day Winning Rate': round(day_win_rate, 4),
                           'Month Winning Rate': round(month_win_rate, 4),
                           'Annualized Volatility': round(volatility, 4),
                           'Mean Turnover': round(mean_turnover, 4)}
        return back_test_ratio



    def single_factor_test(self,factor_df):
        date_list=factor_df.index[::5].tolist()
        group_result_dict=self.get_stock_weight_by_lay(factor_df, holding_period=date_list)
        group_account_dict=self.back_test_by_lay(group_result_dict)
        self.get_index_net_value(date_list)
        fig1=self.draw_net_value(group_account_dict)
        performance_df=self.get_all_group_performance(group_account_dict)
        ic_info_concat=self.different_rolling_length_ic_test(factor_df)
        result_dict=self.IC_test(factor_df)
        fig2=self.draw_ic_plot(result_dict)

        #
        factor_name='apm'
        pic_root_addr=r'D:\code\factor_module\factor_handle\factor_report_picture'
        pic_addr1=pic_root_addr+'/'+factor_name+'1.jpg'
        fig1.savefig(pic_addr1)
        pic_addr2=pic_root_addr+'/'+factor_name+'2.jpg'
        fig2.savefig(pic_addr2)
        doc_handle=doc_class()
        doc_handle.write_heading(factor_name)
        doc_handle.write_dataframe(performance_df)
        doc_handle.write_picture(pic_addr1)
        doc_handle.write_dataframe(ic_info_concat)
        doc_handle.write_picture(pic_addr2,height=5)
        doc_root_addr=r'D:\code\factor_module\factor_handle\factor_report'
        doc_addr=doc_root_addr+'/'+factor_name+'.docx'
        doc_handle.save_file(doc_addr)


        # print(ic_info_concat)
        # self.draw_ic_plot(result_dict)
        pass
    pass



