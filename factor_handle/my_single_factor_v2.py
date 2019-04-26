import sys
sys.path.append('C:/code/work_space/import_moduals')
import scipy.stats as sps
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


#%%
class factor_test(object):
    def __init__(self):
        day_data_dict = pd.read_pickle('D:/code/tick_data_handle/factor_test/factor_test_data/day_data_series.pkl').to_dict()
        day_data_dict['adj_close'] = day_data_dict['close'] * day_data_dict['adjfactor']
        day_data_dict['adj_open'] = day_data_dict['open'] * day_data_dict['adjfactor']
        self.day_data_dict=day_data_dict
        self.day_ipo = pd.read_pickle('D:/code/tick_data_handle/prepare_neutralise_data/prepare_data/' + 'ipo_day.pkl')
        self.day_index_dict = pd.read_pickle('D:/code/tick_data_handle/factor_test/factor_test_data/day_index_series.pkl').to_dict()
        self.Zone=''
        pass


    def get_portfolio_performance_rate(self,day_capital_series,turnover_series):
        print(self.Zone)
        # day_capital_series=self.return_rate
        # turnover_series=self.turnover_series

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
        day_capital_df['month'] = day_capital_df.date.apply(lambda x: x[:7])
        # day_capital_df['year'] = day_capital_df.date.apply(lambda x: x[:4])
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

    def according_weight_back_test(self, df_weight, index_name):
        """
        :param df_weight:
        :param index_name:
        :return:
        """
        day_data_dict=self.day_data_dict
        day_index_dict=self.day_index_dict

        st = df_weight.index[0]
        ed = df_weight.index[-1]

        # 从日数据中生成收盘价，开盘价，交易状态，复权因子及隔夜涨跌数据
        df_closeback = day_data_dict['adj_close']
        df_openback = day_data_dict['adj_open']
        df_status = day_data_dict['trade_status']

        df_re_overnight = df_openback / df_closeback.shift(1) - 1
        df_re_intraday = df_closeback / df_openback - 1
        df_close_by_close = df_closeback / df_closeback.shift(1) - 1

        # 需要交易的日期
        exchange_date_list = df_weight.loc[st:ed].index.tolist()
        exchange_date_lag_1_dict = dict(zip(exchange_date_list[1:], exchange_date_list[:-1]))

        # 初始化回测周期，持仓和现金数据
        # 所有交易日
        trade_date_list = df_closeback.loc[st:ed, :].index.tolist()
        trade_date_lag_1_dict = dict(zip(trade_date_list[1:], trade_date_list[:-1]))
        commission = {'buy_cost': 0.000, 'sell_cost': 0.000}

        account = {'position': pd.DataFrame(),
                   'nav_morning': pd.Series(),
                   'nav_evening': pd.Series(),
                   'position_cost': pd.DataFrame(),
                   'abandon_deal': pd.DataFrame(),
                   'buy_fee': pd.Series(),
                   'sell_fee': pd.Series(),
                   'turnover': pd.Series()}

        for date in trade_date_list:
            print(date)
            if date in exchange_date_list:
                # 生成当天禁止购买的股票列表，禁止卖出的股票列表，目标购买的股票列表（目标购买的列表=信号-禁止买入-持有且禁卖）
                target_series = df_weight.loc[date]

                ban_list = df_status.loc[date][df_status.loc[date] != 1].index.tolist()

                df_re_overnight_series: pd.Series = df_re_overnight.loc[date]
                overnight_null_list = df_re_overnight_series[df_re_overnight_series.isnull()].index.tolist()
                limit_up_list = df_re_overnight_series[df_re_overnight_series.gt(0.08)].index.tolist()

                limit_down_list = df_re_overnight_series[df_re_overnight_series.lt(- 0.08)].index.tolist()

                debuy_list = list(set(ban_list) | set(overnight_null_list) | set(limit_up_list))
                desell_list = list(set(ban_list) | set(overnight_null_list) | set(limit_down_list))
                ## 第一天
                if date == trade_date_list[0]:
                    # morning 撮合成交
                    realize_deal = target_series.drop(debuy_list, errors='ignore')
                    abandon_deal = target_series[debuy_list]
                    buy_fee = realize_deal.sum() * commission['buy_cost']
                    realize_deal.name = date
                    account['position'] = account['position'].append(realize_deal.dropna())
                    account['nav_morning'][date] = 1 - buy_fee
                    account['abandon_deal'] = account['abandon_deal'].append(abandon_deal.dropna())
                    account['buy_fee'][date] = buy_fee
                    # account['turnover'][date]=realize_deal.sum()
                    # 晚上算净值
                    return_rate = (account['position'].loc[date] * df_re_intraday.loc[date]).sum()
                    account['nav_evening'][date] = account['nav_morning'][date] + return_rate
                    continue
                    pass
                # 算早上的净值
                last_trade_date = trade_date_lag_1_dict[date]
                nav_morning = account['nav_evening'][last_trade_date]
                return_rate = (df_re_overnight_series * account['position'].loc[last_trade_date]).sum()
                nav_morning = nav_morning + return_rate

                # morning 撮合成交
                last_exchange_date = exchange_date_lag_1_dict[date]
                last_exchange_date_position = account['position'].loc[last_exchange_date]
                expect_deal = target_series.sub(last_exchange_date_position, fill_value=0)
                # 过滤不可能的交易
                expect_buy_deal = expect_deal[expect_deal > 0]
                expect_sell_deal = expect_deal[expect_deal < 0]

                # 先卖后买，保证权重加起来约等于1,如果有股票卖不出，则计算剩余仓位，然后要买入的股票平分剩余仓位。
                realize_sell_deal = expect_sell_deal.drop(desell_list, errors='ignore')
                left_weight = 1 - last_exchange_date_position.add(realize_sell_deal, fill_value=0).sum()

                realize_buy_deal = expect_buy_deal.drop(debuy_list, errors='ignore')
                realize_buy_deal = realize_buy_deal[realize_buy_deal > 0.0001]
                realize_buy_deal.loc[:] = left_weight / realize_buy_deal.dropna().shape[0]
                turnover = (realize_buy_deal.sum() + realize_sell_deal.abs().sum()) * 0.5
                abandon_buy_deal = expect_buy_deal[expect_buy_deal.index.isin(debuy_list)]
                abandon_sell_deal = expect_sell_deal[expect_sell_deal.index.isin(desell_list)]

                realize_deal = realize_buy_deal.append(realize_sell_deal)
                position = last_exchange_date_position.add(realize_deal, fill_value=0)
                abandon_deal = abandon_buy_deal.append(abandon_sell_deal)
                buy_fee = realize_buy_deal.sum() * commission['buy_cost']
                sell_fee = realize_sell_deal.abs().sum() * commission['sell_cost']

                realize_deal.name = date
                abandon_deal.name = date
                position.name = date

                account['position'] = account['position'].append(position.dropna())
                account['nav_morning'][date] = nav_morning - buy_fee - sell_fee
                account['abandon_deal'] = account['abandon_deal'].append(abandon_deal.dropna())
                account['buy_fee'][date] = buy_fee
                account['sell_fee'][date] = sell_fee
                account['turnover'][date] = turnover

                # 晚上算净值,净值用单利算
                return_rate = (account['position'].loc[date] * df_re_intraday.loc[date]).sum()
                account['nav_evening'][date] = account['nav_morning'][date] + return_rate
                pass
            else:
                # 没有发生交易
                last_trade_date = trade_date_lag_1_dict[date]
                return_rate = (account['position'].loc[last_trade_date] * df_close_by_close.loc[date]).sum()
                position = account['position'].loc[last_trade_date]
                position.name = date
                account['position'] = account['position'].append(position)
                account['nav_evening'][date] = account['nav_evening'][last_trade_date] + return_rate
                pass


        nav_evening_series = pd.Series(data=account['nav_evening'], name='long')
        df_index = day_index_dict['close'].loc[st:ed]
        index_nav = df_index[index_name] / df_index[index_name][0]
        index_nav.name = index_name
        df_index.index = nav_evening_series.index
        nav_df:pd.DataFrame = pd.concat([nav_evening_series, index_nav], axis=1)
        nav_df.insert(1, 'hedge', (nav_df['long'] / nav_df[index_name]))
        nav_df['log_hedge'] = np.log(nav_df['hedge'])
        nav_df['log_long'] = np.log(nav_df['long'])
        back_test_ratio = self.get_portfolio_performance_rate(nav_df['hedge'],account['turnover'])
        return {'nav_df':nav_df,'back_test_ratio':back_test_ratio}



    def grouped_factor_cap_line(self,factor_df:pd.DataFrame, holding_period_list, group_num):
        """
        :param factor_df:
        :param holding_period_list:
        :param group_num:
        :return:
        """
        data_dict=self.day_data_dict

        # 获得需要因子分层的日期,signal_date_list为因子分组的日期
        trade_date_list = data_dict['open'].index.tolist()
        trade_date_1day_lag = dict(zip(trade_date_list[:-1], trade_date_list[1:]))
        signal_date_list = factor_df[factor_df.index.isin(holding_period_list)].index.tolist()

        # 生成容器（dict），存放每个分组的权重dataframe
        group_sig_dict = {}
        for group_name in ['group_' + str(x) for x in range(group_num)]:
            group_sig_dict[group_name] = pd.DataFrame()

        for date in signal_date_list:
            daily_factor_series = factor_df.loc[date]
            daily_status_series = data_dict['trade_status'].loc[date]
            daily_factor_series = daily_factor_series[daily_status_series[daily_status_series == 1].index]
            daily_factor_series.dropna(inplace=True)
            daily_factor_series.sort_values(inplace=True)
            group_stock_num = int(len(daily_factor_series) / group_num)
            for group_name in group_sig_dict.keys():
                group = int(group_name.strip('group_'))
                if group == group_num - 1:
                    daily_group_stock_list = daily_factor_series.iloc[group * group_stock_num:].index.tolist()
                else:
                    daily_group_stock_list = daily_factor_series.iloc[
                                             group * group_stock_num: (group + 1) * group_stock_num].index.tolist()
                ## 因子分组要将日期延后一天，形成交易
                daily_group_holding = pd.Series(1.0 / len(daily_group_stock_list), index=daily_group_stock_list,
                                                name=trade_date_1day_lag[date])
                group_sig_dict[group_name] = group_sig_dict[group_name].append(daily_group_holding)
                pass

        # 分层回测
        # group0是因子值最小组
        group_nav_df=pd.DataFrame()
        group_performance_ratio=pd.DataFrame()
        for group_name in group_sig_dict.keys():
            print(group_name)
            sig = group_sig_dict[group_name]
            result_dict = self.according_weight_back_test(sig, '000905.SH')
            result_dict['nav_df'].columns=pd.MultiIndex.from_product([[group_name],result_dict['nav_df'].columns])
            group_nav_df=pd.concat([group_nav_df,result_dict['nav_df']],axis=1)
            group_performance_ratio=group_performance_ratio.append(pd.Series(result_dict['back_test_ratio'],name=group_name))
            pass
        group_result_dict={'group_nav_df':group_nav_df,'group_performance_ratio':group_performance_ratio}


        # # draw plot
        # fig = plt.figure()
        # fig.set_size_inches(12,21)
        # ax1 = fig.add_subplot(311)
        # ax2 = fig.add_subplot(312)
        # ax3 = fig.add_subplot(313)
        # for group in long_nav_df.columns:
        #     ax1.plot(long_nav_df.index, long_nav_df[group], label=group)
        #     ax2.plot(hedge_nav_df.index, hedge_nav_df[group], label=group)
        # ax3.plot(long_short.index, long_short, label='long-short')
        # ax1.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        # ax2.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        # print(performance_ratio_df)
        # return long_nav_df, hedge_nav_df, performance_ratio_df, fig
        return group_result_dict

    def draw_stock_layer_return(self,group_result_dict):
        print(self.Zone)
        # draw plot
        long_nav_df=group_result_dict['group_nav_df'].sort_index(axis=1).loc[slice(None),(slice(None),'log_long')]
        hedge_nav_df=group_result_dict['group_nav_df'].sort_index(axis=1).loc[slice(None),(slice(None),'log_hedge')]
        # hedge_nav_df=group_result_dict['group_nav_df']['log_hedge']
        if hedge_nav_df.iloc[-1, 0] > hedge_nav_df.iloc[-1, -1]:
            long_short = hedge_nav_df.iloc[:, 0] - hedge_nav_df.iloc[:, -1]
        else:
            long_short = hedge_nav_df.iloc[:, -1] - hedge_nav_df.iloc[:, 0]
        fig = plt.figure()
        fig.set_size_inches(12,21)
        ax1 = fig.add_subplot(311)
        ax2 = fig.add_subplot(312)
        ax3 = fig.add_subplot(313)
        for group in long_nav_df.columns:
            ax1.plot(long_nav_df.index, long_nav_df[group], label=group)
        for group in hedge_nav_df.columns:
            ax2.plot(hedge_nav_df.index, hedge_nav_df[group], label=group)
        ax3.plot(long_short.index, long_short, label='long-short')
        ax1.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        ax2.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        plt.show()
        # print(performance_ratio_df)
        # return long_nav_df, hedge_nav_df, performance_ratio_df, fig
        pass



    def Rank_Correlation(self,factor_df, return_df, significant = 0.05):
        print(self.Zone)
        #创建空列表
        corr=pd.Series([],name='corr')
        p_values=pd.Series([],name='p_values')

        #每月计算因子与收益率的相关系数
        for date in factor_df.index:
            #将每月的因子和次月的收益率合并为dataframe
            factor_return_df=pd.DataFrame({'x':factor_df.loc[date], 'y':return_df.loc[date]})
            factor_return_df.dropna(how='any',inplace=True)
            #计算每月的秩相关系数
            spear = sps.spearmanr(factor_return_df)
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
        # print(result_dict)
        # if plot:
        #     #画出相关系数图像
        #     corr_significant = all_corr['corr'][all_corr['p_values']<0.05]
        #     corr_non_significant = all_corr['corr'][all_corr['p_values']>=0.05]
        #     corr_graph = pd.DataFrame({'corr_significant':corr_significant,'corr_non_significant':corr_non_significant})
        #     x_axis = pd.to_datetime(corr_graph.index)
        #     fig = plt.figure()
        #     fig.set_size_inches(15, 10)
        #     ax1 = fig.add_subplot(111)
        #     ax1.bar(x_axis,corr_graph['corr_significant'],width = 5,color = 'green',label='Significant IC')
        #     ax1.bar(x_axis,corr_graph['corr_non_significant'],width = 5,color = 'red',label='Non_Significant IC')
        #     ax1.legend(bbox_to_anchor=(1.0, 1), loc=2, borderaxespad=0.)
        #     plt.show()
        #     return (result_dict,fig)
        # #print("平均IC：%s，IC标准差：%s，年化IR：%s，正相关显著比例：%s，负相关显著比例：%s，同向显著次数占比：%s，状态切换占比：%s" % corr_result)
        # else:
        #     return (result_dict,0)
        return result_dict


    def IC_test(self,factor_df, holding_period):
        data_dict=self.day_data_dict
        day_tday=data_dict['adj_open'].index.tolist()
        if type(holding_period) in [type([]),type(pd.Series())]:
            signal_date = factor_df.index[factor_df.index.isin(holding_period)].tolist()
            buy_date = [day_tday[day_tday.index(i) + 1] for i in signal_date]
            sell_date = [holding_period[holding_period.index(i) + 1] for i in signal_date]
        else:
            signal_date = [factor_df.index[i] for i in range(0, len(factor_df.index), holding_period)]
            buy_date = [day_tday[day_tday.index(i)+1] for i in signal_date]
            sell_date = [day_tday[day_tday.index(i)+holding_period] for i in signal_date]


        df_re_overnight = data_dict['adj_open'] / data_dict['adj_close'].shift(1) - 1
        df_re_overnight=df_re_overnight.loc[signal_date]

        buy_status_cond = data_dict['trade_status'].loc[buy_date]==1
        sell_status_cond = data_dict['trade_status'].loc[sell_date]==1
        buy_status_cond.index = signal_date
        sell_status_cond.index = signal_date
        overnight_cond = df_re_overnight<=0.09
        # ipo_cond = day_ipo.loc[signal_date]>60
        # all_cond = (buy_status_cond & sell_status_cond & overnight_cond & ipo_cond)
        all_cond = (buy_status_cond & sell_status_cond & overnight_cond )

        buy_date_open = data_dict['adj_open'].loc[buy_date]
        sell_date_close = data_dict['adj_close'].loc[sell_date]
        buy_date_open.index =signal_date
        sell_date_close.index = signal_date

        signal_df = (factor_df.loc[signal_date]).loc[all_cond.index,all_cond.columns][all_cond==True]
        holding_rate = (sell_date_close / buy_date_open)-1
        result_dict = self.Rank_Correlation(signal_df, holding_rate, significant = 0.05)
        return result_dict

    def draw_ic_plot(self,result_dict):
        print(self.Zone)
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
        pass

    # def period_ic_test(self,factor_df, factor_mean = 1):
    #     period_lt = [1,2,3,4,5,20]
    #     ic_info= pd.DataFrame()
    #
    #     for period in period_lt:
    #         mean_factor_df = factor_df.rolling(factor_mean, min_periods = int(round(0.8*factor_mean,0))).mean()
    #         mean_factor_df.dropna(how='all',inplace=True)
    #         result_dict = self.IC_test(mean_factor_df,period)
    #         result_series=pd.Series(result_dict,name=str(period))
    #         ic_info=ic_info.append(result_series)
    #     return ic_info

    def single_factor_test(self,factor_df):
        date_list=factor_df.index[::5].tolist()
        group_result_dict=self.grouped_factor_cap_line(factor_df,holding_period_list=date_list,group_num=10)
        self.draw_stock_layer_return(group_result_dict)
        result_dict=self.IC_test(factor_df,5)
        self.draw_ic_plot(result_dict)
        pass


    pass

#%%
if __name__=='__main__':
    factor_MIDF = pd.read_pickle('D:/code/tick_data_handle/hdf_to_factor/force_compare_ratio.pkl')
    factor_df1=factor_MIDF.iloc[:,0].unstack().T
    factor_df1.columns=factor_df1.columns.map(lambda x:x[:6]+'.SZ' if x[-4:]=='XSHE' else x[:6]+'.SH')
    factor_df1=factor_df1.iloc[:100]

    FT=factor_test()
    FT.single_factor_test(factor_df1)

    pass

