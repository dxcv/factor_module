from factor_handle.get_preprocess_necessary_data import *


# m=MongoDB_io()

@ print_func_name
def get_stock_pre_adj_price():
    m.set_db('stock_daily_data')
    m.set_collection('stock_pre_price')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_date, end_date=end_date)
    return df
    pass

@ print_func_name
def get_zz500_price():
    m.set_db('index_daily_data')
    m.set_collection('000905_XSHG')
    df=m.read_data_to_get_dataframe_include_condition(start_date=start_date, end_date=end_date)
    df.set_index('date',inplace=True)
    return df
    pass