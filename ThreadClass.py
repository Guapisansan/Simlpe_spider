import ctypes
import inspect
import threading
import time
from loguru import logger


class ThreadClass(object):
    @staticmethod
    def no_parame_thread_run(function_name, thread_num):
        """
        没有参数的线程封装
        :param function_name:函数名 function
        :param thread_num: 线程数量
        """
        thread_list = []
        for i in range(thread_num):
            thread_list.append(threading.Thread(target=function_name))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

    @staticmethod
    def parame_unchanged_thread_run_many(functionData: list, isJoin: bool):
        """
        参数不改变
        @staticmethod 静态方法与函数单独执行效果相同
        多线程 多函数跑可配置，参数
        :param functionData: [{函数名: 线程数量}]
        """
        thread_list = []
        for data in functionData:
            function = data.get('function')
            thread_num = data.get('thread_num')
            args = data.get('args')
            if args:
                for i in range(thread_num):
                    thread_list.append(threading.Thread(target=function, name=function.__name__, args=args))
            else:
                for i in range(thread_num):
                    thread_list.append(threading.Thread(target=function, name=function.__name__))
        for thread in thread_list:
            thread.start()
        if isJoin is True:
            for thread in thread_list:
                thread.join()

    @staticmethod
    def parame_changed_thread_run_many(functionData: list, isJoin: bool):
        """
        参数可改变
        @staticmethod 静态方法与函数单独执行效果相同
        多线程 多函数跑可配置，参数
        :param functionData: [{函数名: 线程数量}]
        :param isJoin: 是否等待
        """
        thread_list = []
        for data in functionData:
            function = data.get('function')
            thread_num = data.get('thread_num')
            args = data.get('args')
            if args:
                if type(args) is list:
                    for param in args:
                        thread_list.append(threading.Thread(target=function, name=function.__name__, args=param))
                else:
                    for i in range(thread_num):
                        thread_list.append(threading.Thread(target=function, name=function.__name__, args=args))
            else:
                for i in range(thread_num):
                    thread_list.append(threading.Thread(target=function, name=function.__name__))
        for thread in thread_list:
            thread.start()
        if isJoin is True:
            for thread in thread_list:
                thread.join()

    def monitoring_thread(self, *args):
        """
        线程监控
        """
        pass

    def _async_raise(self, tid, exctype):

        """raises the exception, performs cleanup if needed"""

        tid = ctypes.c_long(tid)

        if not inspect.isclass(exctype):
            exctype = type(exctype)

        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))

        if res == 0:
            raise ValueError("invalid thread id")

        elif res != 1:

            # """if it returns a number greater than one, you're in trouble,

            # and you should call it again with exc=NULL to revert the effect"""

            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)

            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self, thread_id):
        try:
            self._async_raise(thread_id, SystemExit)
            logger.info("Stopping thread: %s" % thread_id)
        except ValueError:
            pass