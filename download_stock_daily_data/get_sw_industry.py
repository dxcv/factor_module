from data_base.mongodb import MongoDB_io
from download_stock_daily_data.basical_func import *

def get_sw_industry():
    logging_joinquant()
    df=get_industries(name='sw_l1')
    df=df.append(get_industries(name='sw_l2'))
    df=df.append(get_industries(name='sw_l3'))
    df.index.name='industry_code'
    df.reset_index(inplace=True)

    # 插入数据库
    m=MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('sw_industry_code')
    m.remove_all_documents_from_mongodb()
    m.insert_dataframe_to_mongodb(df)

if __name__=='__main__':
    get_sw_industry()
    pass