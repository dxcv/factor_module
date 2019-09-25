from jqdatasdk import *
import json



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