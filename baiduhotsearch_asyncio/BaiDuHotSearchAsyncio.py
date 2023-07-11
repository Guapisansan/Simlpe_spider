import json
import sys
import threading
import os

import requests
import schedule
import asyncio

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from parsel import Selector
from loguru import logger
from SpiderClass import AsyncSpider
from  database import SessionLocal
from RedisQueueClass import RedisQueue
from setting import *

logger.add(os.path.join(rootPath, "log_folder", os.path.split(__file__)[-1] + ".log"), retention='7 days')

details_page_queue = RedisQueue("details_page_queue")




class BaiDuHotSearchAsyncio(AsyncSpider):
    def __init__(self):
        super(BaiDuHotSearchAsyncio, self).__init__()

    def run(self):
        logger.info("百度热搜电影爬取！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
        task = self.analysis_list_page()  # 解析列表页, 生成详情页异步任务
        self.run_asyncio(task)  # 运行异步任务


    def scheduled_tasks(self):
        """
        定时任务
        :return:
        """
        self.run()
        schedule.every().monday.do(self.run)
        while True:
            schedule.run_pending()

    def analysis_list_page(self):
        params = {
            'tab': 'movie',
        }
        response = requests.get(url=hot_search, params=params, headers=headers)
        if response.status_code != 200:
            logger.error("请求失败： {}".format(params))
        else:
            select = Selector(response.text)
            # self.save_html(response_text=response.text)
            page_list = select.xpath('''//div[@class='category-wrap_iQLoo ']''').getall()
            detail_tasks = []
            for row in page_list:
                select = Selector(row)
                name = select.xpath('''//div[@class='category-wrap_iQLoo ']/div/a/div/text()''').get()
                type = select.xpath('''//div[@class='category-wrap_iQLoo ']/div/div[1]/text()''').get()
                actor = select.xpath('''//div[@class='category-wrap_iQLoo ']/div/div[2]/text()''').get()
                introduction = select.xpath('''//div[@class='category-wrap_iQLoo ']/div/div[3]/text()''').get()
                detail_href = select.xpath('''//div[@class='category-wrap_iQLoo ']/div/div[3]/a/@href''').get()
                detail_dict = {
                    "name": name,
                    "type": type,
                    "actor": actor,
                    "introduction": introduction,
                    "detail_href": detail_href
                }
                detail_tasks.append(detail_dict)
            return self.build_detail_task(detail_tasks)

    def build_detail_task(self, detail_tasks):
        """
        构建详情页任务
        :param detail_task:
        :return:
        """
        detail_urls = [detail_task.get("detail_href") for detail_task in detail_tasks]
        load_task = self.load_asyncio_task(urls=detail_urls, call_back_func=self.analysis_detail_page)  # 异步加载
        return load_task

    def analysis_detail_page(self, task):
        data = task.result()
        print(data)


if __name__ == '__main__':
    task = BaiDuHotSearchAsyncio()
    task.scheduled_tasks()
