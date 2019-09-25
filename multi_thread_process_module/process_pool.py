
from multiprocessing import Pool
import time
import random
import os

def task(num):
    time.sleep(random.uniform(0.1, 1))  # 同步程序
    print("%s:%s" % (num, os.getpid()))
    return num


if __name__ == "__main__":
    p = Pool(2)
    for i in range(20):
        res = p.apply(task, args=(i,))
        print("--->", res)
    # 完完全全的同步程序,等上面走完了在执行finish
    print("finish")
