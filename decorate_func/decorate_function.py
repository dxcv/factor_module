import time

def cal_time(f):
    def inner(*args, **kwargs):
        # 函数前执行
        t1 = time.time()
        print(f.__name__)
        ret = f(*args, **kwargs)
        # 函数后执行
        t2 = time.time()
        print('consume time: ', t2 - t1)
        return ret
        pass
    return inner
    pass