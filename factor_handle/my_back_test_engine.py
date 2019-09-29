import pandas as pd
import numpy as np

class back_test_engine(object):
    def __init__(self):
        self.stock_daily_price=None
        self.index_daily_data=None
        self.trade_status=None
        self.nothing=None
        self.commission={'buy_cost': 0.000, 'sell_cost': 0.000}
        self.back_test_mode={'single_interest':True,'compound_interest':False}
        self.index_net_value=None


    def according_weight_back_test(self, stock_weight):
        """
        :param stock_weight:
        :return:
        """
        stock_price:pd.DataFrame=self.stock_daily_price
        trade_status = self.trade_status


        st = stock_weight.index[0]
        ed = stock_weight.index[-1]

        # 从日数据中生成收盘价，开盘价，交易状态，复权因子及隔夜涨跌数据
        close_back_adj = stock_price[['close','date','stock']].set_index(['date','stock']).close.unstack()
        open_back_adj = stock_price[['open','date','stock']].set_index(['date','stock']).open.unstack()

        overnight_return = open_back_adj / close_back_adj.shift(1) - 1
        intraday_return = close_back_adj / open_back_adj - 1
        close_by_close_return = close_back_adj / close_back_adj.shift(1) - 1

        # 需要交易的日期
        exchange_date_list = stock_weight.loc[st:ed].index.tolist()
        exchange_date_lag_1_dict = dict(zip(exchange_date_list[1:], exchange_date_list[:-1]))

        # 初始化回测周期，持仓和现金数据
        # 所有交易日
        trade_date_list = close_back_adj.loc[st:ed, :].index.tolist()
        trade_date_lag_1_dict = dict(zip(trade_date_list[1:], trade_date_list[:-1]))
        commission = self.commission

        account_dict = {'position': pd.DataFrame(),
                       'nav_morning(after_trade)': pd.Series(),
                       'nav_evening': pd.Series(),
                       'abandon_deal': pd.DataFrame(),
                       'buy_fee': pd.Series(),
                       'sell_fee': pd.Series(),
                       'turnover': pd.Series()}

        for date in trade_date_list:
            print(date)
            if date in exchange_date_list:
                # 生成当天禁止购买的股票列表，禁止卖出的股票列表，目标购买的股票列表（目标购买的列表=信号-禁止买入-持有且禁卖）
                target_series = stock_weight.loc[date]
                ban_list = trade_status.loc[date][trade_status.loc[date] != True].index.tolist()
                overnight_return_series: pd.Series = overnight_return.loc[date]
                overnight_null_list = overnight_return_series[overnight_return_series.isnull()].index.tolist()
                limit_up_list = overnight_return_series[overnight_return_series.gt(0.08)].index.tolist()
                limit_down_list = overnight_return_series[overnight_return_series.lt(- 0.08)].index.tolist()
                debuy_list = list(set(ban_list) | set(overnight_null_list) | set(limit_up_list))
                desell_list = list(set(ban_list) | set(overnight_null_list) | set(limit_down_list))
                ## 第一天
                if date == trade_date_list[0]:
                    # morning 撮合成交
                    realize_deal = target_series.drop(debuy_list, errors='ignore')
                    abandon_deal = target_series[debuy_list]
                    buy_fee = realize_deal.sum() * commission['buy_cost']
                    realize_deal.name = abandon_deal.name = date
                    account_dict['position'] = account_dict['position'].append(realize_deal.dropna())
                    account_dict['nav_morning(after_trade)'][date] = 1 - buy_fee
                    account_dict['abandon_deal'] = account_dict['abandon_deal'].append(abandon_deal.dropna())
                    account_dict['buy_fee'][date] = buy_fee
                    account_dict['sell_fee'][date] = 0
                    account_dict['turnover'][date]=realize_deal.sum()*0.5
                    # 晚上算净值,单利模式
                    return_rate = (account_dict['position'].loc[date] * intraday_return.loc[date]).sum()
                    account_dict['nav_evening'][date] = account_dict['nav_morning(after_trade)'][date] + return_rate
                    continue
                    pass
                # 算早上的交易前净值
                last_trade_date = trade_date_lag_1_dict[date]
                nav_morning_before_trade = account_dict['nav_evening'][last_trade_date]
                return_rate = (overnight_return_series * account_dict['position'].loc[last_trade_date]).sum()
                nav_morning_before_trade = nav_morning_before_trade + return_rate

                # morning 撮合成交
                last_exchange_date = exchange_date_lag_1_dict[date]
                last_exchange_date_position = account_dict['position'].loc[last_exchange_date]
                expect_deal = target_series.sub(last_exchange_date_position, fill_value=0)
                # 期望的交易
                expect_buy_deal = expect_deal[expect_deal > 0]
                expect_sell_deal = expect_deal[expect_deal < 0]
                # 期望交易中被禁止的交易
                abandon_buy_deal = expect_buy_deal[expect_buy_deal.index.isin(debuy_list)]
                abandon_sell_deal = expect_sell_deal[expect_sell_deal.index.isin(desell_list)]
                # # 上期仓位中需要卖出被禁止卖出，需要买入被禁止的部分
                # abandon_sell_position = last_exchange_date_position[last_exchange_date_position.index.isin(desell_list)&
                #                                                     last_exchange_date_position.index.isin(expect_sell_deal.index)]
                # abandon_buy_position = last_exchange_date_position[last_exchange_date_position.index.isin(debuy_list)&
                #                                                    last_exchange_date_position.index.isin(expect_buy_deal.index)]


                # 先卖后买，保证权重加起来约等于1
                realize_sell_deal = expect_sell_deal.drop(desell_list, errors='ignore')
                # # 上期仓位先减去禁止卖出的部分
                # left_weight = 1 - abandon_sell_position.sum()
                # # 再减去禁止买入的部分
                # left_weight = left_weight - abandon_buy_position.sum()
                # #
                # left_weight = left_weight - last_exchange_date_position.add(realize_sell_deal, fill_value=0).sum()

                realize_buy_deal = expect_buy_deal.drop(debuy_list, errors='ignore')
                realize_buy_deal = realize_buy_deal[realize_buy_deal > 0.0001]
                realize_buy_deal = (realize_sell_deal.abs().sum()/realize_buy_deal.sum()) * realize_buy_deal
                turnover = (realize_buy_deal.sum() + realize_sell_deal.abs().sum()) * 0.5


                realize_deal = realize_buy_deal.append(realize_sell_deal)
                position = last_exchange_date_position.add(realize_deal, fill_value=0)
                abandon_deal = abandon_buy_deal.append(abandon_sell_deal)
                buy_fee = realize_buy_deal.sum() * commission['buy_cost']
                sell_fee = realize_sell_deal.abs().sum() * commission['sell_cost']

                realize_deal.name = date
                abandon_deal.name = date
                position.name = date

                account_dict['position'] = account_dict['position'].append(position.dropna())
                account_dict['nav_morning(after_trade)'][date] = nav_morning_before_trade - buy_fee - sell_fee
                account_dict['abandon_deal'] = account_dict['abandon_deal'].append(abandon_deal.dropna())
                account_dict['buy_fee'][date] = buy_fee
                account_dict['sell_fee'][date] = sell_fee
                account_dict['turnover'][date] = turnover

                # 晚上算净值,净值用单利算
                return_rate = (account_dict['position'].loc[date] * intraday_return.loc[date]).sum()
                account_dict['nav_evening'][date] = account_dict['nav_morning(after_trade)'][date] + return_rate
                pass
            else:
                # 没有发生交易
                last_trade_date = trade_date_lag_1_dict[date]
                return_rate = (account_dict['position'].loc[last_trade_date] * close_by_close_return.loc[date]).sum()
                position = account_dict['position'].loc[last_trade_date]
                position.name = date
                account_dict['position'] = account_dict['position'].append(position)
                account_dict['nav_evening'][date] = account_dict['nav_evening'][last_trade_date] + return_rate
                pass
            pass
        return account_dict


    pass


