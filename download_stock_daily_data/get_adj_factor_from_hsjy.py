#%%
from data_base.mysql_db import Mysql_Class
import pandas as pd
from data_base.mongodb import MongoDB_io

path = r'D:\code\factor_module\download_stock_daily_data\adj_factor_hsjy' + '/'
# factor_name='RatioAdjustingFactor'


def down_load_data():
    sql=Mysql_Class()
    sql.connect_database('hsjy1')
    sql_query='SELECT * FROM gildata.SecuMain'
    df=sql.read_database(sql_query)
    df.to_csv(path+'SecuMain.csv')
    sql_query='SELECT * FROM gildata.QT_AdjustingFactor'
    df=sql.read_database(sql_query)
    df.to_csv(path+'adj_factor.csv')
    return

def merge_data():
    SecuMain=pd.read_csv(path+'SecuMain.csv',index_col=0)
    SecuMain_=SecuMain[SecuMain.SecuCategory==1]
    adj_factor=pd.read_csv(path+'adj_factor.csv',index_col=0)
    merge_df:pd.DataFrame=pd.merge(adj_factor[['ExDiviDate','InnerCode','RatioAdjustingFactor']],SecuMain_[['InnerCode','SecuCode']],how='inner',on='InnerCode')
    temp:pd.Series=merge_df.set_index(['ExDiviDate','SecuCode']).RatioAdjustingFactor
    pivot_df=temp.unstack()
    return pivot_df

def edit_index_and_fill_na(df:pd.DataFrame):
    # region Description:edit columns and index
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_ipo_date')
    ipo_df=m.read_data_to_get_dataframe()
    ipo_df['stock_short_name']=ipo_df.stock.apply(lambda x:x[:6])
    map_series=ipo_df.set_index('stock_short_name').stock
    df=df.reindex(map_series.index,axis=1)
    df.columns=df.columns.map(lambda x:map_series[x])
    df.loc['1990-01-01 00:00:00',:]=1
    df.sort_index(inplace=True)
    df.index=pd.to_datetime(df.index)
    # endregion

    # region Description:
    df.fillna(method='ffill',inplace=True)
    m.set_collection('stock_trade_date')
    trade_date=m.read_data_to_get_dataframe()
    trade_date_list=trade_date.date.tolist()
    df=df.reindex(trade_date_list,axis=0)
    df.fillna(method='ffill',inplace=True)
    # endregion

    return df
    pass


pivot_data=merge_data()
adj_factor=edit_index_and_fill_na(pivot_data)
adj_factor.to_pickle(path+'adj_factor_data.pkl')
pass


