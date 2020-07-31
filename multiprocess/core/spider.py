#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import ctypes
import pycurl
import random
import threading
import time
import urllib
from io import BytesIO
from multiprocessing import Process, Value, Queue, Lock
import chardet
import pymongo
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm


class SuccessResult(Exception):
    pass


class Seed(object):
    def __init__(self, value, retries=3, type = None, last_time=time.time(), rest_time=0,
                 is_faid_task_write=True, is_ok=False):
        self.retries = retries
        self.value = value
        self.type = type
        self.last_time = last_time
        self.rest_time = rest_time
        self.is_faid_task_write = is_faid_task_write
        self.is_ok = is_ok

    def __str__(self):
        return str(self.value) + "," + str(self.type) + "," + str(self.is_ok)

    def ok(self):
        self.is_ok = True

    def is_ok(self):
        return self.is_ok

    def retry(self):
        self.retries = self.retries - 1


class ThreadMonitor(threading.Thread):
    def __init__(self, total, comlete, lock, interval=1, bar_name=None, ):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.total = total
        self.comlete = comlete
        self.interval = interval
        self.bar_name = bar_name
        self.lock = lock

    def run(self):
        with tqdm(total=self.total, desc=self.bar_name) as pbar:
            last_size = 0
            while True:
                end = time.time() + self.interval
                with self.lock:
                    current_size = self.comlete.value
                pbar.update(current_size - last_size)
                if time.time() < end:
                    time.sleep(end - time.time())
                last_size = current_size


class SpiderManger(object):
    def __init__(self, spider_num, mongo_config, complete_timeout=5*60,
                 job_name=None, log_config=None, use_new_download_api=False
                 , proxies_pool=None, pycurl_config=None, use_proxy=True,**kwargs):
        self.use_proxy = use_proxy
        self.pycurl_config = pycurl_config
        self.proxies_pool = proxies_pool
        self.use_new_download_api = use_new_download_api
        self.complete_timeout = complete_timeout
        self.job_name = job_name
        self.seeds_queue = Queue()
        self.seeds_queue.cancel_join_thread()
        self.comlete = Value(ctypes.c_int, 0)
        self.lock = Lock()
        self.spider_num = spider_num
        self.spider_list = []
        self.mongo_config = mongo_config
        import logging
        logging.basicConfig(**log_config)
        self.log = logging
        self.log.info("output_db_collection: " + self.mongo_config["db"] + ","+ self.mongo_config["collection"])
        self.db_collect = None
        self.used_proxy = None
        for i in range(self.spider_num):
            self.spider_list.append(Process(target=self.run, name="Spider-" + str(i)))

    def fetch_task(self, timeout=None):
        if timeout:
            seed = self.seeds_queue.get(timeout=self.complete_timeout)
        else:
            seed = self.seeds_queue.get(timeout=timeout)
        if seed.retries > 0:
            if time.time() - seed.last_time < seed.rest_time:
                self.seeds_queue.put(seed)
                return None
            else:
                self.progress_increase()
                return seed
        else:
            if seed.is_faid_task_write:
                self.write([{"_status": 3, "_seed": seed}])
            return None

    def progress_increase(self, step=1):
        with self.lock:
            self.comlete.value += step

    def progress_decrease(self, step=1):
        with self.lock:
            self.comlete.value -= step

    def overwrite_download_opt(self, pycurl):
        pass

    @staticmethod
    def convert_proxy_format(proxy="http://u0:crawl@192.168.0.71:3128"):
        password = proxy[proxy.find("//") + 2: proxy.find("@")]
        proxy = proxy.replace(password + "@", "")
        return proxy, password

    def download(self, request):
        request_url = request.get("url")
        headers = request.get("headers")
        if isinstance(headers, dict):
            headers = [k+":"+v for k,v in headers.items()]
        proxies = request.get("proxy")
        mothed = request.get("mothed")
        encoding = request.get("encoding")

        c = pycurl.Curl()
        body = BytesIO()
        if self.pycurl_config:
            #default
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.TIMEOUT, 3)
            c.setopt(pycurl.CONNECTTIMEOUT, 1)
            c.setopt(pycurl.URL, request_url)
            if headers:
                c.setopt(pycurl.HTTPHEADER, headers)
            c.setopt(pycurl.ENCODING, 'gzip,deflate')
            c.setopt(pycurl.SSL_VERIFYPEER, False)
            c.setopt(pycurl.SSL_VERIFYHOST, False)
            if mothed is None:
                mothed = "get"
            if mothed.lower() == "post":
                c.setopt(pycurl.POST, 1)
                data = request.get("data")
                if data:
                    c.setopt(pycurl.POSTFIELDS, urllib.urlencode(data))
            c.setopt(pycurl.WRITEFUNCTION, body.write)
            if self.use_proxy:
                if proxies:
                    proxy, password = self.convert_proxy_format(proxies)
                    self.log.debug((proxy,password))
                    c.setopt(pycurl.PROXY, proxy)
                    c.setopt(pycurl.PROXYUSERPWD, password)
                else:
                    if self.used_proxy:
                        proxy, password = self.convert_proxy_format(self.used_proxy)
                        self.log.debug((proxy, password ))
                        c.setopt(pycurl.PROXY, proxy)
                        c.setopt(pycurl.PROXYUSERPWD, password)
            #set pycurl_config
            for k, v in self.pycurl_config.items():
                c.setopt(k, v)
            # set yourself
            self.overwrite_download_opt(c)
        else:
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.TIMEOUT, 3)
            c.setopt(pycurl.CONNECTTIMEOUT, 1)
            c.setopt(pycurl.URL, request_url)
            if headers:
                c.setopt(pycurl.HTTPHEADER, headers)
            c.setopt(pycurl.ENCODING, 'gzip,deflate')
            c.setopt(pycurl.SSL_VERIFYPEER, False)
            c.setopt(pycurl.SSL_VERIFYHOST, False)
            if mothed is None:
                mothed = "get"
            if mothed.lower() == "post":
                c.setopt(pycurl.POST, 1)
                data = request.get("data")
                if data:
                    c.setopt(pycurl.POSTFIELDS, urllib.urlencode(data))
            c.setopt(pycurl.WRITEFUNCTION, body.write)
            if self.use_proxy:
                if proxies:
                    proxy, password = self.convert_proxy_format(proxies)
                    self.log.debug((proxy, password))
                    c.setopt(pycurl.PROXY, proxy)
                    c.setopt(pycurl.PROXYUSERPWD, password)
                else:
                    if self.used_proxy:
                        proxy, password = self.convert_proxy_format(self.used_proxy)
                        self.log.debug((proxy, password))
                        c.setopt(pycurl.PROXY, proxy)
                        c.setopt(pycurl.PROXYUSERPWD, password)

            self.overwrite_download_opt(c)
        try:
            c.perform()
            code = c.getinfo(pycurl.HTTP_CODE)
            if code != 200:
                raise pycurl.error(code, "")
        except pycurl.error as err:
            #if err[0] not in (7,28,56):
             #   self.log.error(err)
            self.log.exception(err)
            #raise err
            return ""
        finally:
            c.close()
        result = body.getvalue()
        if not encoding:
            coding = chardet.detect(result)['encoding']
        return result.decode(coding)

    def do_request(self, request):
        max_retries = request.get("max_retries", 3)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=max_retries))
        s.mount('https://', HTTPAdapter(max_retries=max_retries))
        try:
            r = s.request(**request)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            self.log.exception(e)
            return ""


    def make_request(self, seed):
        pass

    def parse_item(self, content, seed):
        pass

    def process(self, seed):
        try:
            request_dict = self.make_request(seed)
        except Exception as e:
            self.log.exception(e)
            request_dict = None
            seed.retries = seed.retries - 1
            if seed.retries > 0:
                if self.rest_time:
                    time.sleep(self.rest_time)
                self.seeds_queue.put(seed)
                self.progress_decrease()
            else:
                return [{"_status": 3, "_seed": seed}]
        if request_dict:
            if self.use_new_download_api:
                content = self.download(request_dict)
            else:
                content = self.do_request(request_dict)
            if content == "":
                seed.retries = seed.retries - 1
                if seed.retries > 0:
                    if self.rest_time:
                        time.sleep(self.rest_time)
                    self.seeds_queue.put(seed)
                    self.progress_decrease()
                    #失败后切换代理
                    #self.used_proxy = random.choice(self.proxies_pool)
                    #return [{"_status": 3,"_seed":seed}]
                else:
                    return [{"_status": 1,"_seed": str(seed)}]
            else:
                try:
                    result = self.parse_item(content, seed)
                    if result:
                        if isinstance(result, list):
                            result = list(map(lambda x: dict(list(x.items()) + [("_status", 0), ("_seed",str(seed))]),
                                              result))
                            return result
                        else:
                            result.update({"_status":0,"_seed":str(seed)})
                            return [result]
                    else:
                        return [{"_status": 4, "_seed": str(seed)}]
                except Exception as e:
                    return [{"_status": 2, "_seed": str(seed)}]

    def run(self):
        client = pymongo.MongoClient(self.mongo_config["addr"])
        self.db_collect = client[self.mongo_config["db"]][self.mongo_config["collection"]]
        if self.proxies_pool:
            self.used_proxy = random.choice(self.proxies_pool)

        while True:
            try:
                seed = self.fetch_task(self.complete_timeout)
            except Exception as e:
                self.log.info("fetch task over time {}, spider will shutdown normally!")
                break
            try:
                self.process(seed)
                if not seed.is_ok():
                    seed.retry()
            except Exception as e:
                seed.retry()
                self.log.exception(e)
                continue
        client.close()

    def write(self, documents):
        self.db_collect.insert_many(documents)

    def main_loop(self, show_process=True):
        if show_process:
            m = ThreadMonitor(self.seeds_queue.qsize(), self.comlete, lock=self.lock, bar_name="进度")
            m.start()
        for p in self.spider_list:
            p.start()
        for p in self.spider_list:
            p.join()



