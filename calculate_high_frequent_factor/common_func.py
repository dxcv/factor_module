import os


def check_stock_is_done(stock,stock_file_path):
    stock_file_list=os.listdir(stock_file_path)
    done_stock_list=list(map(lambda x:x[1:].strip('.csv'),stock_file_list))
    if stock in done_stock_list:
        return True
    else:
        return False
    pass

# path=r'D:\code\factor_module\factor_storage\factor0001\factor0001_01\s'
# cond=check_stock_is_done('000001',path[:-1])
# print(cond)