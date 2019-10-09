from download_stock_daily_data.get_real_market_data import get_market_price_class


if __name__=='__main__':
    a = get_market_price_class()
    a.drop_duplicate_document(fq='post')
    pass