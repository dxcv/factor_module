from jqdatasdk import *
import json
import os

def change_wording_address():
    os.chdir(r'D:\code\factor_module\download_stock_daily_data')
    pass

def get_stock_code_list():
    return get_all_securities(types='stock', date=None).index.tolist()

def get_trade_date_list(start_date_str):
    return get_trade_days(start_date=start_date_str, end_date=None, count=None)


def logging_joinquant():
    with open('joinquant_password.json','r') as f:
        dic=json.load(f)
        auth(dic['username'],dic['password'])
        pass
    pass

def get_setting_start_date():
    with open('start_date.json','r') as f:
        dic=json.load(f)
        start_date=dic['start_date']
        pass
    return start_date