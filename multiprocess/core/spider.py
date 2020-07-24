#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import requests
import json
import random
from multiprocessing import Process, Value, Manager
import re
import chardet
import os
import redis
import pymongo
import sys
from requests.adapters import HTTPAdapter
from tqdm import tqdm
import ctypes
import threading
import time
from fake_useragent import UserAgent
if sys.version_info >= (3, 0):
    from collections import UserDict
else:
    from UserDict import UserDict


class RequestSpiderError(Exception):
    pass


class ParseContentError(Exception):
    pass


class MaxRetriesError(Exception):
    pass


class SuccessResult(Exception):
    pass


class Seed(object):
    def __init__(self, value, retries=3, type = None):
        self.retries = retries
        self.value = value
        self.type = type

    def __str__(self):
        return str(self.value) + "," + str(self.retries) + "," + str(self.type)


class ThreadMonitor(threading.Thread):
    def __init__(self, total, comlete, interval=1, bar_name=None):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.total = total
        self.comlete = comlete
        self.interval = interval
        self.bar_name = bar_name

    def run(self):
        with tqdm(total=self.total, desc=self.bar_name) as pbar:
            last_size = 0
            while True:
                end = time.time() + self.interval
                current_size = self.comlete.value
                pbar.update(current_size - last_size)
                if time.time() < end:
                    time.sleep(end - time.time())
                last_size = current_size


class SpiderManger(object):
    def __init__(self, spider_num, mongo_config, completetimeout=5*60, retries=3,
                 job_name=None, proxies=None, log_config=None, request_timeout=3, headers=None,
                 sleep_interval=-1, requet_retries=3, rest_time=0.1):
        self.rest_time = rest_time
        self.requet_retries = requet_retries
        self.sleep_interval = sleep_interval
        self.request_timeout = request_timeout
        self.completetimeout = completetimeout
        self.ua = UserAgent()
        self.headers = headers
        self.job_name = job_name
        self.manger = Manager()
        self.seeds_queue = self.manger.Queue()
        self.comlete = Value(ctypes.c_int, 0)
        self.proxy = proxies
        self.spider_num = spider_num
        self.spider_list = []
        self.mongo_config = mongo_config
        import logging
        logging.basicConfig(**log_config)
        self.log = logging

        for i in range(self.spider_num):
            self.spider_list.append(Process(target=self.run, name="Spider-" + str(i)))

    def get_request(self, request_url, encoding=None):
        headers = {"User-Agent": self.ua.chrome}
        if self.headers:
            headers.update(self.headers)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=self.requet_retries))
        s.mount('https://', HTTPAdapter(max_retries=self.requet_retries))
        try:
            if self.proxy:
                r = s.get(url=request_url,
                             headers=headers, proxies={"http": random.choice(self.proxy)}, timeout=self.request_timeout)
            else:
                r = s.get(url=request_url,
                             headers=headers, timeout=self.request_timeout)
            if r.status_code == requests.codes.ok:
                if encoding is None:
                    encoding = chardet.detect(r.content)['encoding']
                page = r.content.decode(encoding)
                return page
            else:
                r.raise_for_status()
        except Exception as f:
            self.log.info(f)
            return ""

    def make_requset_url(self, seed):
        raise NotImplementedError

    def parse_item(self, content, seed):
        raise NotImplementedError

    def process(self, seed):
        url = self.make_requset_url(seed)
        if url:
            content = self.get_request(url)
            if content == "":
                seed.retries = seed.retries - 1
                if seed.retries > 0:
                    self.seeds_queue.put(seed)
                    self.comlete.value -= 1
                    time.sleep(self.rest_time)
                    return [{"_status": 3,"_seed":seed.value}]
                else:
                    return [{"_status": 1,"_seed":seed.value}]
            else:
                try:
                    result = self.parse_item(content, seed)
                    if result:
                        if isinstance(result, list):
                            result = list(map(lambda x : dict(list(x.items())+[("_status",0),("_seed",seed.value)]),result))
                            return result
                        else:
                            result.update({"_status":0,"_seed":seed.value})
                            return [result]
                except Exception as e:
                    return [{"_status": 2,"_seed":seed.value}]

    def run(self):
        client = pymongo.MongoClient(self.mongo_config["addr"])
        mongo_collcetion = client[self.mongo_config["db"]][self.mongo_config["collection"]]
        while True:
            try:
                seed = self.seeds_queue.get(timeout=self.completetimeout)
                self.comlete.value += 1
                if self.sleep_interval != -1:
                    time.sleep(self.sleep_interval)
            except Exception as e:
                self.log.info("job done !")
                break
            try:
                documents = self.process(seed)
            except Exception as e:
                self.log.info("parse error, continue! " + str(e))
                continue
            try:
                if documents and documents[0]["_status"] != 3:
                    mongo_collcetion.insert_many(documents)
            except Exception as e:
                self.log.info("insert db error, continue! " + str(e))
                continue
        client.close()

    def main_loop(self, show_process=True):
        if show_process:
            m = ThreadMonitor(self.seeds_queue.qsize(), self.comlete)
            m.start()
        for p in self.spider_list:
            p.start()
        for p in self.spider_list:
            p.join()



