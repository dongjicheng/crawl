#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import requests
import json
import random
from multiprocessing import Process, Value, Queue,Manager,Lock
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
import pycurl
from io import BytesIO
import urllib


from collections import UserDict


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
    def __init__(self, spider_num, mongo_config, complete_timeout=5*60, retries=3,
                 job_name=None, log_config=None, request_timeout=3,
                 sleep_interval=None, requet_retries=3, rest_time=None, write_seed=True, use_new_download_api=False
                 , proxies_pool=None, pycurl_config=None, use_proxy=True):
        self.retries = retries
        self.use_proxy = use_proxy
        self.pycurl_config = pycurl_config
        self.proxies_pool = proxies_pool
        self.use_new_download_api = use_new_download_api
        self.rest_time = rest_time
        self.requet_retries = requet_retries
        self.sleep_interval = sleep_interval
        self.request_timeout = request_timeout
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
        self._write_seed = write_seed
        for i in range(self.spider_num):
            self.spider_list.append(Process(target=self.run, name="Spider-" + str(i)))

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

    def get_request(self, request):
        request_url = request.get("url")
        headers = request.get("headers")
        if request.get("proxy"):
            proxies = request.get("proxy")
        else:
            proxies = request.get("proxies")
        encoding = request.get("encoding")
        mothed = request.get("mothed")
        data = request.get("data")
        json = request.get("json")
        verify = request.get("verify")
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=self.requet_retries))
        s.mount('https://', HTTPAdapter(max_retries=self.requet_retries))
        try:
            if mothed:
                if mothed.lower() == "get":
                    r = s.get(url=request_url,
                              headers=headers, proxies=proxies, timeout=self.request_timeout,verify=verify)
                elif mothed.lower() == "post":
                    r = s.post(url=request_url, data=data, json=json,
                              headers=headers, proxies=proxies, timeout=self.request_timeout, verify=verify)
            else:
                r = s.get(url=request_url,
                             headers=headers, proxies=proxies, timeout=self.request_timeout, verify=verify)
            if r.status_code == requests.codes.ok:
                if encoding is None:
                    encoding = chardet.detect(r.content)['encoding']
                page = r.content.decode(encoding)
                return page
            else:
                r.raise_for_status()
        except Exception as f:
            self.log.exception(f)
            return ""

    def make_request(self, seed):
        raise NotImplementedError

    def parse_item(self, content, seed):
        raise NotImplementedError

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
                content = self.get_request(request_dict)
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
                    return [{"_status": 1,"_seed":seed}]
            else:
                try:
                    result = self.parse_item(content, seed)
                    if result:
                        if isinstance(result, list):
                            result = list(map(lambda x: dict(list(x.items()) + [("_status", 0), ("_seed",seed)]),
                                              result))
                            return result
                        else:
                            result.update({"_status":0,"_seed":seed})
                            return [result]
                    else:
                        return [{"_status": 4,"_seed":seed}]
                except Exception as e:
                    return [{"_status": 2,"_seed":seed}]

    def run(self):
        client = pymongo.MongoClient(self.mongo_config["addr"])
        self.db_collect = client[self.mongo_config["db"]][self.mongo_config["collection"]]
        if self.proxies_pool:
            self.used_proxy = random.choice(self.proxies_pool)

        while True:
            try:
                seed = self.seeds_queue.get(timeout=self.complete_timeout)
                self.progress_increase()
                if self.sleep_interval:
                    time.sleep(self.sleep_interval + random.random() / 10)
            except Exception as e:
                self.log.info("job done !")
                break
            try:
                documents = self.process(seed)
            except Exception as e:
                self.log.exception(e)
                continue
            try:
                if documents and documents[0]["_status"] != 3:
                    self.write(documents, self._write_seed)
            except Exception as e:
                self.log.exception(e)
                continue
        client.close()

    def write(self, documents, write_seed=True, seed_name="_seed"):
        if not write_seed and seed_name:
            [document.pop(seed_name) for document in documents if document["_status"] == 0]
        self.db_collect.insert_many(documents)

    def main_loop(self, show_process=True):
        if show_process:
            m = ThreadMonitor(self.seeds_queue.qsize(), self.comlete, lock=self.lock, bar_name="进度")
            m.start()
        for p in self.spider_list:
            p.start()
        for p in self.spider_list:
            p.join()



