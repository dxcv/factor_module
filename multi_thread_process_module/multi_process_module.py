from multiprocessing import Pool,Process


class MyProcess(Process):
    def __init__(self, target, args=None, kwargs={}):
        # 必须调用一下父类的初始化构造方法.
        super().__init__()
        self.target=target
        self.args=args
        self.kwargs=kwargs
        pass

    # 必须使用叫做run的方法
    def run(self):
        if 'sem' in self.kwargs.keys():
            """
            信号量模式
            """
            self.kwargs['sem'].acquire()
            self.target(*self.args)
            self.kwargs['sem'].release()
        else:
            self.target(*self.args)
        pass




    pass

class MyPool(object):
    def __init__(self,num):
        self.pool=Pool(num)
        pass

    def apply(self,func,args=None,kwargs={}):
        return self.pool.apply(func,args=args,kwds=kwargs)
        pass

    def apply_async(self,func,args=None,kwargs={}):
        return self.pool.apply_async(func,args=args,kwds=kwargs)
        pass

    pass



# def task(num):
#     time.sleep(random.uniform(0.1, 1))  # 同步程序
#     print("%s:%s" % (num, os.getpid()))
#     return num
#
#
# if __name__ == "__main__":
#     p = MyPool()
#     for i in range(20):
#         res = p.apply(task, args=(i,))
#         print("--->", res)
#     # 完完全全的同步程序,等上面走完了在执行finish
#     print("finish")






