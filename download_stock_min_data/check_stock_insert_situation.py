from data_base.mongodb import MongoDB_io
import pandas as pd

m=MongoDB_io()
m.set_db('stock_min_data')
collection_list=m.list_collection_names()
m.close_MongoDB_connection()
collection_list.sort()
insert_date_df=pd.DataFrame()
for stock in collection_list:
    print(stock,collection_list.index(stock))
    m.set_db('stock_min_data')
    m.set_collection(stock)
    df=m.read_data_to_get_field(field={'DATETIME':1})
    series_index=df.DATETIME.astype(str).apply(lambda x:x[:10]).drop_duplicates().tolist()
    insert_date_series=pd.Series(True,index=series_index)
    insert_date_series.name=stock
    insert_date_df=insert_date_df.append(insert_date_series)
    m.close_MongoDB_connection()
    pass

path=r'D:\code\factor_module\download_stock_min_data\stock_insert_situation.csv'
insert_date_df.to_csv(path)