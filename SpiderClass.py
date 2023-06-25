import os
import random
import sys
from datetime import datetime

import pytz

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath)
from ThreadClass import ThreadClass
import json
import time
import traceback
from abc import ABC, abstractmethod
import threading
from loguru import logger
import requests
import base64
import re
import asyncio
import aiohttp

from parsel import Selector, SelectorList


def is_retry(queue_data, number):
    """
    注意：此方法需要redis队列中为json类型才可以添加重试字段
    """
    queue_data = json.loads(queue_data)
    if queue_data.get("retry_count") is None:
        queue_data["retry_count"] = number
        return queue_data
    if queue_data.get("retry_count") == 0:
        # 发飞书
        warning = "以重试5次请求失败: " + json.dumps(queue_data)
        logger.warning(warning)
        return False
    else:
        queue_data["retry_count"] -= 1
        return queue_data


def requests_decorator(function):
    def inner(*args, queue_data=None, callback_queue=None, isRetry=None, **kwargs):
        """
        请求装饰器： 用于处理异常，回调，重试等机制
        :param callback_queue: RedisQueue队列名称
        :param queue_data: dict 重新插入队列
        :param isRetry: 是否重试
        """
        try:
            response = function(*args, **kwargs)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                return False
            else:
                logger.warning("Request failed 响应码为{} {}，查看ip是否可以正常访问".format(response.status_code, response.url))
                if callback_queue:
                    # 重试机制
                    if isRetry is not None:
                        queue_data = is_retry(queue_data, 5)
                        if queue_data is False:
                            return False
                    if response.status_code != 451:
                        time.sleep(300)
                        logger.warning("状态码异常： 等待300秒")
                    callback_queue.sadd(queue_data)
                return False
        except requests.exceptions.InvalidSchema:
            logger.error("requests.exceptions.InvalidSchema为 {}".format(kwargs.get("request_url")))
        except requests.exceptions.InvalidURL:
            logger.error("requests.exceptions.InvalidURL: {}".format(kwargs.get("request_url")))
        except TypeError:
            logger.error(traceback.format_exc())
        except Exception:
            if 'requests.exceptions.ReadTimeout' in traceback.format_exc():
                logger.error('requests.exceptions.ReadTimeout')
            elif 'requests.exceptions.ConnectTimeout' in traceback.format_exc():
                logger.error('requests.exceptions.ReadTimeout')
            else:
                logger.error(traceback.format_exc())
                return False
            if callback_queue:
                callback_queue.sadd(queue_data)
            time.sleep(30)
            return False

    return inner


def requests_decorator_gpt(function):
    def inner(*args, queue_data=None, callback_queue=None, isRetry=None, **kwargs):
        """
        请求装饰器： 用于处理异常，回调，重试等机制
        :param callback_queue: RedisQueue队列名称
        :param queue_data: dict 重新插入队列
        :param isRetry: 是否重试
        """

        def log_error(message):
            logger.error(message)

        try:
            response = function(*args, **kwargs)
            if response.status_code == 200:
                return response
            if response.status_code == 404:
                return False

            log_error("Request failed 响应码为{} {}，查看ip是否可以正常访问".format(response.status_code, response.url))
            if callback_queue:
                # 重试机制
                if isRetry is not None:
                    queue_data = is_retry(queue_data, 5)
                    if queue_data is False:
                        return False

                if response.status_code != 451:
                    time.sleep(300)
                    log_error("状态码异常： 等待300秒")

                callback_queue.sadd(queue_data)

            return False

        except requests.exceptions.InvalidSchema:
            log_error("requests.exceptions.InvalidSchema为 {}".format(kwargs.get("request_url")))

        except requests.exceptions.InvalidURL:
            log_error("requests.exceptions.InvalidURL: {}".format(kwargs.get("request_url")))

        except TypeError:
            log_error(traceback.format_exc())

        except requests.exceptions.ReadTimeout:
            log_error('requests.exceptions.ReadTimeout')
            if callback_queue:
                callback_queue.sadd(queue_data)
            time.sleep(30)
            return False

        except requests.exceptions.ConnectTimeout:
            log_error('requests.exceptions.ReadTimeout')
            if callback_queue:
                callback_queue.sadd(queue_data)
            time.sleep(30)
            return False

        except Exception:
            log_error(traceback.format_exc())
            if callback_queue:
                callback_queue.sadd(queue_data)
            time.sleep(30)
            return False

    return inner


class SpiderClass(ABC, ThreadClass):
    """
    继承ThreadClass类，实现多线程爬虫类，里面包括了定时任务，解析列表页，解析详情页，请求方法
    """

    def __init__(self):
        self.session_pool = [requests.session() for _ in range(1000)]

    def run(self):
        print("Starting ................")

    def scheduled_tasks(self):
        """
        定时任务
        :return:
        """
        self.run()

    @abstractmethod
    def analysis_list_page(self, *args):
        """
        @abstractmethod 装饰器表示， 子类必须包含此方法不然报错
        解析列表页
        :param args:
        :return:
        """
        pass

    @abstractmethod
    def analysis_details_page(self, *args):
        """
         @abstractmethod 装饰器表示， 子类必须包含此方法不然报错
        解析详情页
        :param args:
        :return:
        """
        pass

    @requests_decorator
    def request_api_back_queue(self, request_url: str, headers: dict):
        """
        GET request
        :param request_url: 请求路径
        :param headers: headers
        :param callback_queue: RedisQueue队列名称
        :param queue_data: dict 重新插入队列
        """
        session = random.choice(self.session_pool)
        api_response = session.get(request_url, headers=headers, timeout=15)
        session.close()
        return api_response

    @requests_decorator
    def request_api_params_back_queue(self, request_url: str, params, headers: dict):
        """
        GET request
        :param request_url: 请求路径
        :param headers: headers
        :param callback_queue: RedisQueue队列名称
        :param queue_data: dict 重新插入队列
        """
        session = random.choice(self.session_pool)
        api_response = session.get(request_url, headers=headers, timeout=15, params=params)
        session.close()
        return api_response

    @requests_decorator
    def request_api_post_back_queue(self, request_url: str, payload, headers: dict):
        """
        POST request
        :param request_url: 请求路径
        :param payload: post参数
        :param headers: headers
        :param callback_queue: RedisQueue队列名称
        :param queue_data: dict 重新插入队列
        """
        session = random.choice(self.session_pool)
        api_response = session.post(request_url, headers=headers, timeout=15, json=payload)
        session.close()
        return api_response

    def monitoring_thread(self):
        """
        线程监控函数，可在线程运行中查看正在运行的线程数量
        注意： 如果使用，while循环需要加入break跳出条件
        :return:
        """
        while True:
            thread_list_info = [i.name for i in threading.enumerate() if i.is_alive()]
            thread_dict_info = {}
            for i in thread_list_info:
                if i not in thread_dict_info:
                    thread_dict_info[i] = 1
                else:
                    thread_dict_info[i] += 1
            time.sleep(60)

    def stream_download_file(self, url, file_name):
        """
        流式下载文件防止内存溢出， 适用于大文件下载
        :param url:  下载地址
        :param file_name:  文件名
        :return:
        """
        with requests.get(url, stream=True, timeout=3600) as response:
            with open(file_name, "wb") as file:  # 下载rpm包
                for content in response.iter_content(chunk_size=1024):  # 写入文件
                    file.write(content)  # 写入文件

    def save_html(self, response_text):
        """
        保存html文件, 用于调试准确定位
        """
        with open("gpss.html", "w", encoding="utf-8") as f:
            f.write(response_text)
        logger.success("save_html文件成功")

    def filter_emoji_pattern(self, string):
        """
        去除emoji
        :param string: 字符串
        :return: 在字符串中去除emoji
        """
        emoji_pattern = re.compile(u'[\U00010000-\U0010ffff]')
        sen1 = emoji_pattern.sub('', string)
        return sen1

    def find_all_file(self, pathName: str):
        """
        :param pathName: 输入文件路径
        返回文件夹下的所有文件，以列表的形式
        """
        filepaths = []
        dirpaths = []
        for root, dirs, files in os.walk(pathName):
            for file in files:
                file_path = os.path.join(root, file)
                filepaths.append(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                dirpaths.append(dir_path)
        return filepaths

    def analysis_base64(self, src):
        """
        解析base64
        :param src:  base64字符串
        :return:
        """
        result = re.search("data:image/(?P<ext>.*?);base64,(?P<data>.*)", src, re.DOTALL)
        if result:
            ext = result.groupdict().get("ext")
            data = result.groupdict().get("data")
        else:
            raise Exception("Do not parse!")
        img = base64.urlsafe_b64decode(data)
        return img

    @staticmethod
    def clear_string(string):
        string = string.replace("\r", "").replace("\n", "").replace("\t", "").strip()
        return string

    def get_nested_value(self, data, keys, default=None):
        """
        从多层字典中获取值
        :param data: 多层字典
        :param keys: 字典键列表，例如 ['key1', 'key2', 'key3']，表示 data[key1][key2][key3]
        dict = {a: "a", b: {c: "c", d: {e: "e"}}}
        :param default: 默认值，如果取值失败返回该值，默认为 None
        :return: 取到的值或默认值
        """
        try:
            for key in keys:
                data = data[key]
            return data
        except (KeyError, TypeError):
            return default

    def utc_to_beijing(self, utc_time_str):
        # 查找字符串中是否包含 "."
        dot_index = utc_time_str.find(".")
        if dot_index > 0:
            # 如果包含 ".", 则截取到秒级别
            utc_time_str = utc_time_str[:dot_index] + "Z"
        utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%SZ')
        # 转换时区为北京时间
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = utc_time.replace(tzinfo=pytz.utc).astimezone(beijing_tz)

        # 格式化输出北京时间字符串（忽略毫秒级别）
        beijing_time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
        return beijing_time

class ParselWrapper:
    """
    解析器封装, 用于解析xpath和css
    """

    def __init__(self, html):
        self.selector = Selector(html)

    def xpath(self, xpath_str):
        """
        解析xpath
        :param xpath_str:  xpath表达式
        :return:  返回列表
        """
        result = self.selector.xpath(xpath_str)
        if isinstance(result, Selector):
            return ParselWrapper._selector_to_list(result)
        elif isinstance(result, SelectorList):
            return [ParselWrapper._selector_to_list(i) for i in result]
        else:
            return []

    def css(self, css_str):
        """
        解析css
        :param css_str:
        :return
        """
        result = self.selector.css(css_str)
        if isinstance(result, Selector):
            return ParselWrapper._selector_to_list(result)
        elif isinstance(result, SelectorList):
            return [ParselWrapper._selector_to_list(i) for i in result]
        else:
            return []

    @staticmethod
    def _selector_to_list(selector):
        # 将Selector对象转换为列表
        if selector is None:
            return None
        elif selector.root is None:
            return None
        elif selector.root.getchildren():
            return [ParselWrapper._selector_to_dict(i) for i in selector]
        else:
            return selector.get()

    @staticmethod
    def _selector_to_dict(selector):
        # 将Selector对象转换为字典
        tag = selector.root.tag
        text = selector.get()
        attributes = selector.root.attrib
        children = [ParselWrapper._selector_to_dict(i) for i in selector]
        return {'tag': tag, 'text': text, 'attributes': attributes, 'children': children}


class AsyncSpider:
    """
    异步爬虫
    """

    def __init__(self, urls, max_concurrency=10, retries=3, timeout=30):
        self.urls = urls
        self.max_concurrency = max_concurrency
        self.retries = retries
        self.timeout = timeout
        self.failed_urls = []

    async def fetch(self, session, url):
        retries = self.retries
        while retries > 0:
            try:
                async with session.get(url, timeout=self.timeout) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        retries -= 1
                        await asyncio.sleep(5)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                retries -= 1
                await asyncio.sleep(5)
        self.failed_urls.append(url)
        return None

    async def run(self):
        async with aiohttp.ClientSession() as session:
            tasks = []
            semaphore = asyncio.Semaphore(self.max_concurrency)
            for url in self.urls:
                async with semaphore:
                    tasks.append(asyncio.ensure_future(self.fetch(session, url)))
            responses = await asyncio.gather(*tasks)
            for response in responses:
                if response:
                    print(response)
            if self.failed_urls:
                print(f"Failed URLs: {self.failed_urls}")

