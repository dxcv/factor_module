"""
WQ001
1 (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5)
2 提前15日
3 计算全市场
4 输入day_rate，day_adj_close（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq001(day_rate=day_rate, day_adj_close=day_adj_close):
    data_1 = day_rate.rolling(20, min_periods=15).std()
    data_1[(day_rate >= 0) & (data_1.isna() == 0)] = day_adj_close[(day_rate >= 0) & (data_1.isna() == 0)]
    data_1 = data_1 ** 2
    data_2 = data_1.rolling(5, min_periods=5).apply(lambda x: list(x).index(max(x)))
    data_2_r = data_2.rank(axis=1, method='max')
    data_2_r = (data_2_r.sub(data_2_r.min(axis=1), axis=0)).divide(data_2_r.max(axis=1) - data_2_r.min(axis=1), axis=0)
    return data_2_r - 0.5


"""
WQ004
1 (-1 * Ts_Rank(rank(low), 9))
2 提前9日
3 计算全市场
4 输入day_rate，day_adj_close（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq004(day_adj_low=day_adj_low):
    def rank(x):
        x_sort = np.sort(x)
        rank = np.where(x_sort == x[-1])[0][-1]
        return rank / 8

    day_low_r = day_adj_low.rank(axis=1, method='max')
    day_low_r = (day_low_r.sub(day_low_r.min(axis=1), axis=0)).divide(day_low_r.max(axis=1) - day_low_r.min(axis=1),
                                                                      axis=0)
    day_low_r_tsr = day_low_r.rolling(9, min_periods=9).apply(rank)
    day_low_r_tsr = -1 * day_low_r_tsr
    return day_low_r_tsr


"""
WQ012
1 (sign(delta(volume, 1)) * (-1 * delta(close, 1)))
2 提前1日
3 计算全市场
4 输入day_volume，day_adj_close（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq012(day_volume=day_volume, day_adj_close=day_adj_close):
    delta_close = -1 * (day_adj_close - day_adj_close.shift(1))
    delta_volume = day_volume - day_volume.shift(1)
    delta_volume_sign = pd.DataFrame(1.0, index=delta_volume.index, columns=delta_volume.columns)
    delta_volume_sign[delta_volume < 0] = (-1 * delta_volume_sign)[delta_volume < 0]
    data = delta_volume_sign * delta_close
    return data


"""
WQ024
1 
((((delta((sum(close, 100) / 100), 100) / delay(close, 100)) < 0.05) || ((delta((sum(close, 100) / 100), 100) / delay(close, 100)) == 0.05)) ? (-1 * (close - ts_min(close,100))) : (-1 * delta(close, 3)))
2 提前200日
3 计算全市场
4 输入day_adj_close（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq024(day_adj_close=day_adj_close):
    data_1 = day_adj_close.rolling(100, min_periods=90).mean()
    data_1_rate = (data_1 - data_1.shift(100)) / day_adj_close.shift(100)
    data = -1 * (day_adj_close - day_adj_close.rolling(100, min_periods=90).min())
    data[data_1_rate > 0.05] = (-1 * (day_adj_close - day_adj_close.shift(3)))[data_1_rate > 0.05]
    data[data_1_rate.isna()] = np.nan
    return data


"""
WQ044
1 (-1 * correlation(high, rank(volume), 5))
2 提前5日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq044(day_adj_high=day_adj_high, day_volume=day_volume):
    volume_r = day_volume.rank(axis=1, method='max')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)
    corr_df = day_adj_high.rolling(5, min_periods=5).corr(other=volume_r)
    return -1 * corr_df


"""
WQ050
1 (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))
2 提前5日
3 计算全市场
4 输入day_adj_vwap, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq050(day_adj_vwap=day_adj_vwap, day_volume=day_volume):
    volume_r = day_volume.rank(axis=1, method='max')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)

    vwap_r = day_adj_vwap.rank(axis=1, method='max')
    vwap_r = (vwap_r.sub(vwap_r.min(axis=1), axis=0)).divide(vwap_r.max(axis=1) - vwap_r.min(axis=1), axis=0)

    corr_df = volume_r.rolling(5, min_periods=4).corr(other=vwap_r)
    corr_r = corr_df.rank(axis=1, method='max')
    corr_r = (corr_r.sub(corr_r.min(axis=1), axis=0)).divide(corr_r.max(axis=1) - corr_r.min(axis=1), axis=0)

    corr_tsmax = -1 * (corr_r.rolling(5, min_periods=4).max())
    return corr_tsmax


"""
WQ055
1 (-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low,12)))), rank(volume), 6))
2 提前5日
3 计算全市场
4 输入day_adj_vwap, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq055(day_adj_close=day_adj_close, day_adj_low=day_adj_low, day_adj_high=day_adj_high,
                    day_volume=day_volume):
    ts_min_low = day_adj_low.rolling(12, min_periods=10).min()
    ts_max_high = day_adj_high.rolling(12, min_periods=10).max()
    data_1 = (day_adj_close - ts_min_low) / (ts_max_high - ts_min_low)

    data_1_r = data_1.rank(axis=1, method='max')
    data_1_r = (data_1_r.sub(data_1_r.min(axis=1), axis=0)).divide(data_1_r.max(axis=1) - data_1_r.min(axis=1), axis=0)

    volume_r = day_volume.rank(axis=1, method='max')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)

    corr_df = -1 * volume_r.rolling(6, min_periods=5).corr(other=data_1_r)

    return corr_df


"""
wq084
1 SignedPower(Ts_Rank((vwap - ts_max(vwap, 15)), 21), delta(close, 5))
2 提前5日
3 计算全市场
4 输入day_adj_vwap, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_wq084(day_adj_vwap=day_adj_vwap, day_adj_close=day_adj_close):
    def rank(x):
        if np.isnan(x[-1]):
            return np.nan
        else:
            x_sort = np.sort(x)
            rank = np.where(x_sort == x[-1])[0][-1] + 1
            return rank

    data_1 = day_adj_vwap - day_adj_vwap.rolling(15, min_periods=13).max()
    data_1_tsr = data_1.rolling(21, min_periods=19).apply(rank)
    close_d = day_adj_close - day_adj_close.shift(5)
    data = data_1_tsr ** close_d
    return data