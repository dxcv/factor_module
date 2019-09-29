from factor_handle.my_orthogonality import orthogonality
import os
import pandas as pd

factor_root_addr=r'G:\work_space\pure_factor_deployment\neutralised_factor'
factor_dict={}
start_date='2017-03-01'
end_date='2017-04-01'
file_list=os.listdir(factor_root_addr)
for file in file_list[:5]:
    factor_name=file.strip('.csv')
    file_path=factor_root_addr+'/'+file
    df=pd.read_csv(file_path,index_col=0)
    factor_dict[factor_name]=df.loc[start_date:end_date,:]
    pass

orthogonallity_handle=orthogonality()
orthogonallity_handle.factor_dict=factor_dict
orthogonal_factor=orthogonallity_handle.run_orthogonalization()

