# Simlpe_spider
一个简单生产者消费者模型，通过 redis+多线程+sqlalchemy的技术方法
如果需要使用此框架, 只需要继承SpiderClass.py, 实现自己的爬虫即可,如果想实现分布式,只需要将redis ip设置为0.0.0.0,
多实例进行消费redis队列即可



config.yaml
```
配置文件
```
database.py
```
读取配置文件注册引擎
```
RedisQueue.py
```
redis队列类模型, key前缀为queue:name
封装redis常用方法,如: push, pop, get_size, get_all, get_nowait, get_wait
```
SpiderClass.py
```
爬虫类模型, 通过继承该类实现自己的爬虫,主要功能为 
1. 对request的封装, 异常处理, 重试机制
2. 对通用方法的封装, 如: 获取网页源码,生成html文件,获取目标文件夹所有路径,线程监控
3. 对ThreadClass.py的继承(对开启线程的封装)
```
log_folder文件夹
```
用于存放任务日志, 任务日志以任务名+.log命名
通过loguru模块实现日志的输出
```
baiduhotsearch文件夹
```
1. 此文件夹为一个爬虫实例, 通过继承SpiderClass.py实现
logger.add(os.path.join(rootPath, "log_folder", os.path.split(__file__)[-1] + ".log"), retention='7 days', level="WARNING")
为次爬虫实例添加日志, 日志存放在log_folder文件夹中, 日志名为baiduhotsearch.log

2. 实现SpiderClass.py中的
analysis_details_page, analysis_list_page解析方法
run方法为爬虫的入口
scheduled_tasks方法为定时任务, 通过apscheduler实现

3. 实例化RedisQueue类, 通过redis实现生产者消费者模型

4. 解析列表页,获得详情页url,将url放入redis队列中（analysis_list_page作为此模型的生产者）
这里存入 redis的数据结构最好用sadd方法因为sadd支持去重(redis set集合的特性)
解析详情页,等后续自定义操作(导出,入库等操作)（analysis_details_page作为此模型的消费者） 通过判断redis队列是否为空,来进行任务终止。
```


