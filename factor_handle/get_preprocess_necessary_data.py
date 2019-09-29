from data_base.mongodb import MongoDB_io
import pandas as pd
from decorate_func.decorate_function import print_func_name

start_data='2018-01-01'
end_date='2019-01-01'


m=MongoDB_io()

@ print_func_name
def get_capital_data():
    """
    获得流通市值
    :return:
    """
    m.set_db('stock_daily_data')
    m.set_collection('stock_capital_data')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_data,end_date=end_date)
    return df[['circulating_market_cap','code','date']].set_index(['date','code']).circulating_market_cap.unstack()
    pass

@ print_func_name
def get_stock_industry():
    """
    获得股票行业分类
    :return:
    """
    m.set_db('stock_daily_data')
    m.set_collection('stock_sw_industry_code')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_data,end_date=end_date)
    return df.set_index(['date','stock']).industry_category.unstack()
    pass

@ print_func_name
def get_trade_status():
    m.set_db('stock_daily_data')
    m.set_collection('stock_real_price')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_data,end_date=end_date)
    df['trade_status']=df.volume>1
    return df[['trade_status','stock','date']].set_index(['date','stock']).trade_status.unstack()
    pass

@ print_func_name
def get_ipo_date():
    m.set_db('stock_daily_data')
    m.set_collection('stock_ipo_date')
    df=m.read_data_to_get_dataframe()
    stock_ipo_date_series=df[['stock','start_date']].set_index('stock').start_date

    m.set_collection('stock_trade_date')
    trade_date=m.read_data_to_get_dataframe_include_condition(start_date=start_data,end_date=end_date)
    trade_date_list=trade_date.date.tolist()
    ipo_from_now= pd.DataFrame()
    for stock in stock_ipo_date_series.index:
        time_delta=pd.Series(trade_date_list,index=trade_date_list)-pd.Series(stock_ipo_date_series[stock],index=trade_date_list)
        n_day=time_delta/pd.Timedelta('1 day')
        ipo_from_now[stock]=n_day
        pass
    return ipo_from_now
    pass

@ print_func_name
def get_zz500_weight():
    m.set_db('stock_daily_data')
    m.set_collection('zz500_weight')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_data,end_date=end_date)
    return df[['weight','code','date']].set_index(['date','code']).weight.unstack()

# zz500_weight=get_zz500_weight()
# pass
# circulating_market_cap=get_capital_data()
# stock_industry_code=get_stock_industry()
# trade_status=get_trade_status()
# ipo=get_ipo_date()
#

