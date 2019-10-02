import pymongo
import pandas as pd
from decorate_func.decorate_function import cal_time





class MongoDB_io(object):
    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db_handle=None
        self.collection_handle=None
        pass

    """
    设置数据库
    """
    def set_MongoClient(self,id_address='192.168.0.117',port=27017,username='qjf', password='1234', authSource='admin', authMechanism='SCRAM-SHA-1'):
        self.client=pymongo.MongoClient(host=id_address,port=port,username=username, password=password, authSource=authSource, authMechanism=authMechanism)
        pass

    def set_db(self,db):
        self.db_handle = self.client[db]
        pass

    def set_collection(self,collection):
        self.collection_handle = self.db_handle[collection]
        pass

    def list_collection_names(self):
        db_handle=self.db_handle
        collection_list=db_handle.list_collection_names(session=None)
        return collection_list
        pass

    """
    插入数据
    """

    @ cal_time
    def insert_dataframe_to_mongodb_one_by_one(self, df=pd.DataFrame()):
        collection_handle=self.collection_handle
        for line in range(df.shape[0]):
            if line%1000==0:
                print(line)
            data_series=df.iloc[line]
            collection_handle.insert_one(data_series.to_dict())
        pass

    @ cal_time
    def insert_dataframe_to_mongodb(self, df=pd.DataFrame()):
        collection_handle=self.collection_handle
        if df.shape[0]>200000:
            print('dataframe is too big, please use other function')
            return
        df_data_list=[]
        for line in range(df.shape[0]):
            df_data_list.append(df.iloc[line].to_dict())
            pass
        collection_handle.insert_many(df_data_list)
        pass

    # @ cal_time
    def insert_huge_dataframe_by_block_to_mongodb(self, df=pd.DataFrame(), block_len=100000):
        print('inserting {0} documents'.format(df.shape[0]))
        block_count = 1
        while df.shape[0]:
            print('inserting block ',block_count)
            block_count += 1
            df_component = df.head(block_len)
            self.insert_dataframe_to_mongodb(df_component)
            df = df.iloc[block_len:]
            pass
        pass

    """
    upsert
    """

    @ cal_time
    def upsert_dict_to_mongodb(self, condition=None, upsert_dict=None):
        collection_handle=self.collection_handle
        # collection_handle.update(query=condition, update=upsert_dict,upsert=True,multi=False)
        collection_handle.update_one(condition,upsert_dict, upsert=True)
        pass

    # @ cal_time
    # def insert_dict_to_mongodb(self, data_dict={}):
    #     collection_handle=self.collection_handle
    #     collection_handle.insert_one(data_dict)
    #     pass
    #
    # @ cal_time
    # def insert_dict_list_to_mongodb(self, data_dict_list=[]):
    #     collection_handle=self.collection_handle
    #     collection_handle.insert_many(data_dict_list)
    #     pass



    @ cal_time
    def delete_field_from_mongodb(self,factor_name='bs_ratio'):
        collection_handle=self.collection_handle
        collection_handle.update({},{'$unset':{factor_name:''}},upsert=False,multi=False)
        pass

    """
    delete document
    """

    @ cal_time
    def remove_all_documents_from_mongodb(self):
        collection_handle=self.collection_handle
        collection_handle.delete_many()
        pass

    @ cal_time
    def delete_document_include_condition(self,condition):
        collection_handle=self.collection_handle
        collection_handle.delete_many(condition)
        pass

    """
    read data
    """

    @ cal_time
    def read_data_to_get_dataframe(self):
        collection_handle=self.collection_handle
        cursor = collection_handle.find({})
        data_df = pd.DataFrame(list(cursor))
        if data_df.shape[0]:
            data_df.drop('_id',axis=1,inplace=True)
            print('length of data is {}'.format(data_df.shape[0]))
        return data_df

    @ cal_time
    def read_data_to_get_field(self,field):
        collection_handle=self.collection_handle
        cursor = collection_handle.find({},field)
        data_df = pd.DataFrame(list(cursor))
        if data_df.shape[0]:
            print('length of data is {}'.format(data_df.shape[0]))
        return data_df

    def read_data_from_stock_min_db(self,start_date=None,end_date=None):
        collection_handle=self.collection_handle
        condition1={}
        condition2={}
        if start_date is not None:
            condition1['DATETIME']={'$gt':start_date}
        if end_date is not None:
            condition2['DATETIME']={'$lt':end_date}
        condition={'$and':[condition1,condition2]}

        cursor = collection_handle.find(condition)
        data_df = pd.DataFrame(list(cursor))
        if data_df.shape[0]:
            data_df.drop('_id',axis=1,inplace=True)
            print('length of data is {}'.format(data_df.shape[0]))
        return data_df


    @ cal_time
    def read_data_to_get_dataframe_include_condition(self, start_date=None, end_date=None, stock_list=None):
        collection_handle=self.collection_handle
        condition1={}
        condition2={}
        condition3={}
        if start_date is not None:
            condition1['date']={'$gt':pd.to_datetime(start_date)}
        if end_date is not None:
            condition2['date']={'$lt':pd.to_datetime(end_date)}
        if stock_list is not None:
            condition3['stock']={'$in':stock_list}
        condition={'$and':[condition1,condition2,condition3]}
        cursor = collection_handle.find(condition)
        data_df = pd.DataFrame(list(cursor))
        if data_df.shape[0]:
            data_df.drop('_id',axis=1,inplace=True)
            print('length of data is {}'.format(data_df.shape[0]))
        return data_df
        pass
    pass

    def creat_index(self,index_description=None):
        # index_description = [('stock', 1), ('date', 1)]
        collection_handle=self.collection_handle
        collection_handle.create_index(index_description)
        print('creat index done!')
        pass

    def close_MongoDB_connection(self):
        self.client.close()
        pass

    @ cal_time
    def get_start_end_date(self):
        collection_handle=self.collection_handle
        cursor=collection_handle.find({},{'date'})
        data_df = pd.DataFrame(list(cursor))
        date_series=data_df['date'].drop_duplicates().sort_values()
        start_date=date_series.iloc[0]
        end_date=date_series.iloc[-1]
        return start_date,end_date
        pass

    def get_db_date_list(self):
        collection_handle=self.collection_handle
        cursor=collection_handle.find({},{'date'})
        data_df = pd.DataFrame(list(cursor))
        date_list=data_df['date'].drop_duplicates().sort_values().tolist()
        return date_list
        pass


pass



if __name__=='__main__':
    m=MongoDB_io()
    # m.set_db('tick_factor')

    m.set_MongoClient()

    # # 测试带有筛选条件的查询
    # m.set_collection('bs_ratio')
    # df1=m.read_data_to_get_dataframe_include_condition(start_date='2015-09-01', end_date='2017-01-01')
    # m.set_collection('buy_emotion_factor')
    # df2=m.read_data_to_get_dataframe_include_condition(start_date='2015-09-01', end_date='2017-01-01')

    # # 测试 get_start_end_date 函数
    # m.set_collection('bs_ratio')
    # m.get_start_end_date()

    # # 测试upsert
    # m.set_db('test')
    # m.set_collection('test3')
    # m.upsert_dict_to_mongodb({'a':1},{'$set':{'title':'MongoDB'}})

    # # 测试读取速度
    # m.set_db('stock_daily_data')
    # m.set_collection('stock_daily_market_data')
    # df=m.read_data_to_get_dataframe()

    # 删除符合条件的文档。
    m.set_db('stock_daily_data')
    m.set_collection('stock_capital_data')
    df1=m.read_data_to_get_dataframe_include_condition('2019-03-25')
    m.delete_document_include_condition({'date':{'$gte':pd.to_datetime('2019-03-26')}})


    pass