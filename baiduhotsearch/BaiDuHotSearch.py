import json
import sys
import threading
import os
import schedule


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from parsel import Selector
from loguru import logger
from SpiderClass import SpiderClass
from  database import SessionLocal
from RedisQueueClass import RedisQueue
from setting import *

logger.add(os.path.join(rootPath, "log_folder", os.path.split(__file__)[-1] + ".log"), retention='7 days', level="WARNING")

details_page_queue = RedisQueue("details_page_queue")




class BaiDuHotSearch(SpiderClass):
    def __init__(self):
        super(BaiDuHotSearch, self).__init__()

    def run(self):
        logger.info("百度热搜电影爬取！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
        self.analysis_list_page()
        self.parame_unchanged_thread_run_many([
            {"function": self.analysis_details_page, "thread_num": 1},
            {"function": self.monitoring_thread, "thread_num": 1},
        ], isJoin=True)

    def scheduled_tasks(self):
        """
        定时任务
        :return:
        """
        self.run()
        schedule.every().monday.do(self.run)
        while True:
            schedule.run_pending()

    def analysis_details_page(self):
        while details_page_queue.scrad() != 0:
            data = details_page_queue.spop()
            redis_data = json.loads(data)
            name = redis_data.get("name")
            type = redis_data.get("type")
            actor = redis_data.get("actor")
            introduction = redis_data.get("introduction")
            detail_href = redis_data.get("detail_href")
            response = self.request_api_back_queue(request_url=detail_href,  headers=headers, queue_data=redis_data,
                                                   callback_queue=details_page_queue)
            if response is False:
                logger.error("请求失败： {}".format(detail_href))
            else:
                # 解析,入库其他
                ...

    def analysis_list_page(self):
        params = {
            'tab': 'movie',
        }
        response = self.request_api_params_back_queue(request_url=hot_search, params=params, headers=headers)
        if response is False:
            logger.error("请求失败： {}".format(params))
        else:
            select = Selector(response.text)
            # self.save_html(response_text=response.text)
            page_list = select.xpath('''//div[@class='category-wrap_iQLoo ']''').getall()
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
                details_page_queue.sadd(detail_dict)


if __name__ == '__main__':
    a = BaiDuHotSearch()
    a.scheduled_tasks()
