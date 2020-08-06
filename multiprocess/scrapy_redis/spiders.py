#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import time
from multiprocessing import Process

import psutil
import pymongo
import redis
import tqdm
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider
from scrapy_redis.utils import bytes_to_str


class JiChengSpider(RedisSpider):

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(JiChengSpider, self).from_crawler(crawler, *args, **kwargs)
        return obj

    def make_request_from_data(self, data):
        data = eval(data)
        url = bytes_to_str(data["url"], self.redis_encoding)
        meta = data["meta"]
        return Request(url, meta=meta, dont_filter=True)


class ThreadMonitor(threading.Thread):
    def __init__(self, redis_key, interval=1, bar_name=None):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.redis = redis.Redis(host='192.168.0.117', port=6379, db=0)
        self.redis_key = redis_key
        self.total = self.redis.scard(self.redis_key)
        self.interval = interval
        self.bar_name = bar_name
        self.stop = False

    def stop(self):
        self.stop = True

    def run(self):
        with tqdm(total=self.total, desc=self.bar_name) as pbar:
            last_size = self.total
            while not self.stop:
                end = time.time() + self.interval
                current_size = self.redis.scard(self.redis_key)
                pbar.update(last_size - current_size)
                if time.time() < end:
                    time.sleep(end - time.time())
                last_size = current_size
                if current_size > 0:
                    self.stop()


class ThreadWriter(threading.Thread):
    def __init__(self, interval=1, bar_name=None, buffer_size=512):
        threading.Thread.__init__(self)
        self.stop = False
        self.interval =interval
        self.bar_name = bar_name
        self.buffer_size = buffer_size
        self.buffer = []
        self.counter = 0

    def stop(self):
        self.stop = True

    def write(self, item):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def setup(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        with tqdm(total=self.total, desc=self.bar_name) as pbar:
            while True:
                end = time.time() + self.interval
                current_element = self.redis.lpop(self.redis_key)
                if current_element:
                    self.write(current_element)
                else:
                    self.flush()
                    self.stop()
                pbar.update(1)
                if time.time() < end:
                    time.sleep(end - time.time())
            self.clean_up()


class ThreadFileWriter(ThreadWriter):
    def __init__(self, redis_key, out_file, table_header, interval=1, bar_name=None, buffer_size=512):
        super(ThreadFileWriter, self).__init__(interval=interval, bar_name=bar_name, buffer_size=buffer_size)
        self.redis = redis.Redis(host='192.168.0.117', port=6379, db=0)
        self.redis_key = redis_key
        self.out_file = out_file
        self.out = open(self.out_file, "w")
        self.table_header = table_header
        self.out.write("\t".join(self.table_header) + "\n")

    def stop(self):
        self.stop = True

    def write(self, item):
        self.counter = self.counter + 1
        self.buffer.append(item)
        if self.counter % self.buffer_size == 0:
            self.counter = 0
            self.out.write("\n".join())
            values = ["\t".join([item[key] for key in self.table_header]) for item in self.buffer]
            self.f_out.write("\n".join(values) + "\n")
            self.buffer = []

    def flush(self):
        if self.buffer:
            values = ["\t".join([item[key] for key in self.table_header]) for item in self.buffer]
            self.f_out.write("\n".join(values) + "\n")

    def cleanup(self):
        self.out.close()


class ThreadMongoWriter(ThreadWriter):
    def __init__(self, redis_key, out_mongo_url, collection, interval=1, bar_name=None, buffer_size=512):
        super(ThreadMongoWriter, self).__init__(interval=interval, bar_name=bar_name, buffer_size=buffer_size)
        self.redis = redis.Redis(host='192.168.0.117', port=6379, db=0)
        self.redis_key = redis_key
        self.db = pymongo.MongoClient(out_mongo_url)
        self.out = self.db[collection]

    def stop(self):
        self.stop = True

    def write(self, item):
        self.counter = self.counter + 1
        self.buffer.append(item)
        if self.counter % self.buffer_size == 0:
            self.out.insert_many(self.buffer)
            self.buffer = []
            self.counter = 0

    def flush(self):
        if self.buffer:
            self.out.insert_many(self.buffer)

    def cleanup(self):
        self.db.close()


class ClusterRunner(object):
    def __init__(self, spider_name, thread_writer=None, spider_num=psutil.cpu_count(logical=True), show_process=True):
        #out_uri = "mongodb://192.168.0.13:27017/db?collection=shoujiguishudi"
        self.thread_writer = thread_writer
        self.show_process = show_process
        self.spider_num = spider_num
        from scrapy.cmdline import execute
        self.execute = execute
        self.spider_name = spider_name
        self.redis = redis.Redis(host='192.168.0.117', port=6379, db=0)
        self.spider_list = []
        for i in range(self.spider_num):
            self.spider_list.append(Process(target=self.run_spider, name=self.spider_name + "-" + str(i), kargs={"spider_name": self.spider_name}))

    def set_thread_writer(self, thread_writer):
        self.thread_writer = thread_writer

    def run_spider(self, spider_name):
        self.execute(['scrapy', 'crawl', spider_name])

    def init_start_urls(self):
        raise NotImplementedError

    def run(self):
        #初始化种子URL
        self.init_start_urls()
        #开启监控线程
        if self.show_process:
            m = ThreadMonitor(redis_key=self.spider_name+":pbar", bar_name=self.spider_name)
            m.start()
        # 开启写线程
        self.thread_writer.start()
        #开启爬虫
        for p in self.spider_list:
            p.start()
        for p in self.spider_list:
            p.join()
