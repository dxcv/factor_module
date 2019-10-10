import pymysql as mysql
import pandas as pd

class Mysql_Class(object):
    def __init__(self):
        db_dict=dict()
        db_dict['hsjy1']={'host':'120.132.112.246','port':3306,'username':'fengpan','password':'zugLrLpYjW9u9T2e4w8p','database':'gildata'}
        db_dict['hsjy2']={'host':'120.132.112.246','port':3306,'username':'fengpan','password':'zugLrLpYjW9u9T2e4w8p','database':'gildata_caldb'}
        db_dict['big']={'host':'120.132.112.246','port':3306,'username':'jijing','password':'66W7QSEUNKqz829U','database':'big_data'}
        db_dict['honest']={'host':'14.152.49.155','port':8998,'username':'rootb','password':'3x870OV649AMSn*'}
        db_dict['factor_db']={'host':'120.77.75.168','port':4001,'username':'fengpan','password':'b46Zi5OeY5Qj1N8c4Kw0Ovn0jsnL7w'}
        db_dict['quant_db']={'host':'139.159.176.118','port':3306,'username':'dcr','password':'acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4'}
        self.db_dict=db_dict
        self.conn=None
        pass

    def connect_database(self,db_name=''):
        db_dict=self.db_dict
        host=db_dict[db_name]['host']
        port=db_dict[db_name]['port']
        username=db_dict[db_name]['username']
        password=db_dict[db_name]['password']
        db=db_dict[db_name]['database']
        conn=mysql.connect(host=host,port=port,user=username,password=password,database=db)
        self.conn=conn
        pass

    def read_database(self, sql_query_str=''):
        connection=self.conn
        dataframe=pd.read_sql(sql_query_str, con=connection)
        return dataframe
        pass
    pass

