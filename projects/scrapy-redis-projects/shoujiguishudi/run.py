#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocess.scrapy_redis.spiders import ClusterRunner,ThreadFileWriter,ThreadMongoWriter,ThreadMonitor
import logging
import redis
from scrapy.utils.project import  get_project_settings
class shoujiguishudirunner(ClusterRunner):
    def __init__(self, *args, **kwargs):
        super(shoujiguishudirunner, self).__init__(*args, **kwargs)

    def init_start_urls(self):
        self.redis.delete(self.start_urls_redis_key)
        buffer = []
        buffer_size = 1024
        for i, seed in enumerate(open("shoujiguishudi/resource/buyer_phone.3")):
            if i > 2:
                break
            seed = seed.strip()
            url = "http://shouji.xpcha.com/{0}.html".format(seed)
            data = {"url": url, "meta": {"_seed": seed}}
            buffer.append(str(data))
            if len(buffer) % buffer_size == 0:
                r = self.redis.sadd(self.start_urls_redis_key, *buffer)
                print(r)
                buffer = []
        if buffer:
            r=self.redis.sadd(self.start_urls_redis_key, *buffer)
            print(r)
        self.logger.info("start_urls_redis_key: " + self.start_urls_redis_key)
        self.logger.info("total urls: " + str(self.redis.scard(self.start_urls_redis_key)))

    def get_thread_writer(self):
        thread_writer = ThreadFileWriter(redis_key=self.items_redis_key, bar_name=self.items_redis_key,
                                         out_file="shoujiguishudi/result/shoujiguishudi",
                                       table_header=["phonenumber", "province", "city", "company"])
        thread_writer.setDaemon(True)
        return thread_writer
        # self.writer = ThreadMongoWriter(redis_key=self.spider_name + ":items", out_mongo_url="mongodb://192.168.0.13:27017/jicheng", collection="shoujiguishudi")

    def get_thread_monitor(self):
        thread_monitor = ThreadMonitor(redis_key=self.start_urls_redis_key, bar_name=self.start_urls_redis_key)
        return thread_monitor


if __name__ == '__main__':
    pass
    runner = shoujiguishudirunner(spider_name="shoujiguishudi", spider_num=1)
    runner.init_start_urls()
    #runner.run()
    #from scrapy.cmdline import execute
    #execute(['scrapy', 'crawl', 'shoujiguishudi'])
