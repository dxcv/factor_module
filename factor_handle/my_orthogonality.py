import pandas as pd
import numpy as np
class orthogonality(object):
    def __init__(self):
        self.factor_dict=None
        pass

    def run_orthogonalization(self):
        factor_dict=self.factor_dict
        factor_list=list(factor_dict.keys())
        trade_date=factor_dict[factor_list[0]].index.tolist()
        orthogonal_dict={}
        for factor in factor_list:
            orthogonal_dict[factor]=pd.DataFrame()


        for date in trade_date:
            print(date)
            one_date_factor=pd.DataFrame()
            for factor in factor_list:
                factor_series=factor_dict[factor].loc[date,:]
                factor_series.name=factor
                one_date_factor=one_date_factor.append(factor_series)
                pass
            one_date_factor.dropna(axis=1,inplace=True)
            m = one_date_factor.dot(one_date_factor.T)
            d, u = np.linalg.eig(m)
            d_half = np.diag(d ** (-1 / 2))
            s = u.dot(d_half).dot(u.T)
            s_df=pd.DataFrame(s,index=one_date_factor.index,columns=one_date_factor.index)
            f_reg = one_date_factor.T.dot(s_df).T
            for factor in factor_list:
                orthogonal_factor_series=f_reg.loc[factor]
                orthogonal_factor_series.name=date
                orthogonal_dict[factor] = orthogonal_dict[factor].append(orthogonal_factor_series)
                pass
            pass
        return orthogonal_dict

    pass