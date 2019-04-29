from download_joinquant_data_to_mongodb.get_industry_stocks import *



if __name__=='__main__':
    ## 查看数据插入到哪一天
    m = MongoDB_io()
    m.set_db('stock_daily_data')
    m.set_collection('stock_sw_industry_category')
    start_date, end_date = m.get_start_end_date()
    end_date_str = (end_date + pd.Timedelta('1 day')).strftime('%Y-%m-%d')

    trade_date_list=get_trade_date_list(end_date_str)
    industry_list=get_sw_industry_level1_list()
    for date in trade_date_list:
        print(date)
        one_day_data=get_one_date_industry_data(date,industry_list)
        insert_industry_stocks(one_day_data)
        pass
    pass

pass
