import redis
import time
import json
from database import pool


class RedisQueue(object):
    def __init__(self, name, namespace='queue'):
        self.__db = redis.Redis(connection_pool=pool)
        self.key = '%s:%s' % (namespace, name)

    def qsize(self):
        return self.__db.llen(self.key)  # 返回队列里面list内元素的数量

    def put(self, item):
        self.__db.rpush(self.key, item)  # 添加新元素到队列最右方

    def get_wait(self, timeout=None):
        # 返回队列第一个元素，如果为空则等待至有元素被加入队列（超时时间阈值为timeout，如果为None则一直等待）
        item = self.__db.blpop(self.key, timeout=timeout)
        # if item:
        #     item = item[1]  # 返回值为一个tuple
        return item

    def get_nowait(self):
        # 直接返回队列第一个元素，如果队列为空返回的是None
        item = self.__db.lpop(self.key)
        return item

    def del_key(self):
        self.__db.delete(self.key)

    def sadd(self, item):
        """
        :param item:set集合入库
        """
        if type(item) is dict:
            item = json.dumps(item)
        self.__db.sadd(self.key, item)  # 添加新元素到队列最右方

    def spop(self):
        """
        redis set集合操作
        """
        item = self.__db.spop(self.key)  # 添加新�spop(self.key, item)  # 添加新元素到队列最右方
        return item

    def scrad(self, timeout=None):
        """
        redis set集合操作
        :param timeout: 等待时间
        """
        if self.__db.scard(self.key) == 0:
            if timeout is not None:
                time.sleep(timeout)
        return self.__db.scard(self.key)  # 添加新�spop(self.key, item)  # 添加新元素到队列最右方
