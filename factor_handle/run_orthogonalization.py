from factor_handle.my_orthogonality import orthogonality
import os
import pandas as pd

factor_root_addr=r'D:\code\factor_module\factor_handle\neutralize_z_score_factor'
factor_dict={}

file_list=os.listdir(factor_root_addr)
for file in file_list:
    print(file)
    factor_name=file.strip('.csv')
    ban_list=['tat','roe_yoy','down_vol_per','roa','turnover3m']
    if factor_name in ban_list:
        continue
    file_path=factor_root_addr+'/'+file
    df=pd.read_csv(file_path,index_col=0)
    factor_dict[factor_name]=df
    pass

orthogonallity_handle=orthogonality()
orthogonallity_handle.factor_dict=factor_dict
# orthogonallity_handle.correlation_check()
orthogonal_factor_dict=orthogonallity_handle.run_orthogonalization()

for factor_name in orthogonal_factor_dict.keys():
    print(factor_name)
    orthogonal_factor=orthogonal_factor_dict[factor_name]
    orthogonal_factor.to_csv(r'D:\code\factor_module\factor_handle\orthogonal_factor'+'/'+factor_name+'.csv')
    pass

