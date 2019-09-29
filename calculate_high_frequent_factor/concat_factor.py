import os
import pandas as pd

path=r'D:\code\factor_module\factor_storage\factor0001'
factor_df=pd.DataFrame()
for file in os.listdir(path):
    file_path=path+'/'+file
    stock_df=pd.read_csv(file_path,index_col=0)
    stock_df.columns=[file[1:7]]
    factor_df=factor_df.append(stock_df.T)
    pass
