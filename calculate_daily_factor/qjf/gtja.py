"""
gt001
1  (-1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK(((CLOSE - OPEN) / OPEN)), 6))
2 提前6日
3 计算全市场
4 输入day_adj_close, day_adj_open, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt001(day_volume=day_volume, day_adj_open=day_adj_open, day_adj_close=day_adj_close):
    log_vol = np.log(day_volume)
    log_vol_d_r = (log_vol - log_vol.shift(1)).rank(axis=1, method='max')
    log_vol_d_r = (log_vol_d_r.sub(log_vol_d_r.min(axis=1), axis=0)).divide(
        log_vol_d_r.max(axis=1) - log_vol_d_r.min(axis=1), axis=0)

    data_1 = (day_adj_close - day_adj_open) / day_adj_open
    data_1_r = data_1.rank(axis=1, method='max')
    data_1_r = (data_1_r.sub(data_1_r.min(axis=1), axis=0)).divide(data_1_r.max(axis=1) - data_1_r.min(axis=1), axis=0)

    corr_df = log_vol_d_r.rolling(6, min_periods=5).corr(other=data_1_r)
    return -1 * corr_df


"""
gt005
1  (-1 * TSMAX(CORR(TSRANK(VOLUME, 5), TSRANK(HIGH, 5), 5), 3))
2 提前13日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt005(day_volume=day_volume, day_adj_high=day_adj_high):
    def rank(x):
        if np.isnan(x[-1]):
            return np.nan
        else:
            x_sort = np.sort(x)
            rank = np.where(x_sort == x[-1])[0][-1]
            return rank / 4

    tsr_volume = day_volume.rolling(5, min_periods=4).apply(rank)
    tsr_high = day_adj_high.rolling(5, min_periods=4).apply(rank)
    corr_df = tsr_volume.rolling(5, min_periods=4).corr(other=tsr_high)
    tsmax_corr = corr_df.rolling(3, min_periods=3).max()
    return -1 * tsmax_corr


"""
gt017
1  RANK((VWAP - MAX(VWAP, 15)))^DELTA(CLOSE, 5)
2 提前20日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt017(day_adj_close=day_adj_close, day_adj_vwap=day_adj_vwap):
    data_1 = day_adj_vwap - day_adj_vwap.rolling(15, min_periods=15).max()
    data_1_r = data_1.rank(axis=1, method='max')
    data_1_r = (data_1_r.sub(data_1_r.min(axis=1), axis=0)).divide(data_1_r.max(axis=1) - data_1_r.min(axis=1), axis=0)

    close_d = day_adj_close - day_adj_close.shift(5)
    data = data_1_r ** close_d
    return data


"""
gt074
1  (RANK(CORR(SUM(((LOW * 0.35) + (VWAP * 0.65)), 20), SUM(MEAN(VOLUME,40), 20), 7)) + RANK(CORR(RANK(VWAP), RANK(VOLUME), 6)))
2 提前27日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt074(day_adj_low=day_adj_low, day_adj_vwap=day_adj_vwap, day_volume=day_volume):
    low_p_vwap = 0.35 * day_adj_low + 0.65 * day_adj_vwap
    low_p_vwap_sum = low_p_vwap.rolling(20, min_periods=19).sum()
    volume_mean = day_volume.rolling(40, min_periods=38).mean()
    volume_mean_sum = volume_mean.rolling(20, min_periods=19).sum()

    corr_1 = low_p_vwap_sum.rolling(7, min_periods=6).corr(other=volume_mean_sum)
    corr_1_r = corr_1.rank(axis=1, method='max')
    corr_1_r = (corr_1_r.sub(corr_1_r.min(axis=1), axis=0)).divide(corr_1_r.max(axis=1) - corr_1_r.min(axis=1), axis=0)

    vwap_r = day_adj_vwap.rank(axis=1, method='max')
    vwap_r = (vwap_r.sub(vwap_r.min(axis=1), axis=0)).divide(vwap_r.max(axis=1) - vwap_r.min(axis=1), axis=0)
    volume_r = day_volume.rank(axis=1, method='max')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)

    corr_2 = vwap_r.rolling(6, min_periods=5).corr(other=volume_r)
    corr_2_r = corr_2.rank(axis=1, method='max')
    corr_2_r = (corr_2_r.sub(corr_2_r.min(axis=1), axis=0)).divide(corr_2_r.max(axis=1) - corr_2_r.min(axis=1), axis=0)

    data = corr_1_r + corr_2_r
    return data


"""
gt097
1 STD(VOLUME,10)
2 提前10日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt097(day_volume=day_volume):
    data = day_volume.rolling(10, min_periods=8).std()
    return data


"""
gt150
1 (CLOSE+HIGH+LOW)/3*VOLUME
2 提前0日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt150(day_adj_close=day_adj_close, day_adj_high=day_adj_high, day_adj_low=day_adj_low,
                    day_volume=day_volume):
    data = day_volume * (day_adj_close + day_adj_high + day_adj_low) / 3
    return data


"""
gt176
1 CORR(RANK(((CLOSE - TSMIN(LOW, 12)) / (TSMAX(HIGH, 12) - TSMIN(LOW,12)))), RANK(VOLUME), 6)
2 提前18日
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt176(day_adj_close=day_adj_close, day_adj_low=day_adj_low, day_adj_high=day_adj_high,
                    day_volume=day_volume):
    data_1 = day_adj_close - day_adj_low.rolling(12, min_periods=10).min()
    data_2 = day_adj_high.rolling(12, min_periods=10).max() - day_adj_low.rolling(12, min_periods=10).min()
    data = data_1 / data_2
    data_r = data.rank(axis=1, method='first')
    data_r = (data_r.sub(data_r.min(axis=1), axis=0)).divide(data_r.max(axis=1) - data_r.min(axis=1), axis=0)
    volume_r = day_volume.rank(axis=1, method='first')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)
    corr_df = data_r.rolling(6, min_periods=5).corr(volume_r)
    return corr_df


"""
gt179
1 (RANK(CORR(VWAP, VOLUME, 4)) *RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME,50)), 12)))
2 50
3 计算全市场
4 输入day_adj_high, day_volume（pd.DataFrame），返回日数据（pd.Series）
"""


def calculate_gt179(day_volume=day_volume, day_adj_vwap=day_adj_vwap, day_adj_low=day_adj_low):
    corr_1 = day_adj_vwap.rolling(4, min_periods=3).corr(day_volume)
    low_r = day_adj_low.rank(axis=1, method='first')
    low_r = (low_r.sub(low_r.min(axis=1), axis=0)).divide(low_r.max(axis=1) - low_r.min(axis=1), axis=0)
    mean_volume = day_volume.rolling(50, min_periods=48).mean()
    volume_r = mean_volume.rank(axis=1, method='first')
    volume_r = (volume_r.sub(volume_r.min(axis=1), axis=0)).divide(volume_r.max(axis=1) - volume_r.min(axis=1), axis=0)
    corr_2 = low_r.rolling(12, min_periods=10).corr(volume_r)
    corr_1_r = corr_1.rank(axis=1, method='first')
    corr_1_r = (corr_1_r.sub(corr_1_r.min(axis=1), axis=0)).divide(corr_1_r.max(axis=1) - corr_1_r.min(axis=1), axis=0)
    corr_2_r = corr_2.rank(axis=1, method='first')
    corr_2_r = (corr_2_r.sub(corr_2_r.min(axis=1), axis=0)).divide(corr_2_r.max(axis=1) - corr_2_r.min(axis=1), axis=0)
    data = corr_1_r * corr_2_r

    return data