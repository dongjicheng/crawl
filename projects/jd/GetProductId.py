#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import sys

from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.tools import process_manger
from multiprocess.tools import timeUtil, collections
import random
from fake_useragent import UserAgent
import tqdm
from mongo import op
import urllib


class GetProductId(SpiderManger):
    def __init__(self, **kwargs):
        super(GetProductId, self).__init__(**kwargs)
        self.proxies = list(map(lambda x:("http://u{}:crawl@192.168.0.71:3128".format(x)), range(28)))
        self.ua = UserAgent()
        with op.DBManger() as m:
            last_brand_collect = m.get_lasted_collection("jingdong", filter={"name": {"$regex": r"^brand20\d\d\d\d\d\d$"}})
            pipeline = [
                {"$match": {"cate_id": {"$ne": None}}},
                {"$match": {"brand_id": {"$ne": None}}},
                {"$match": {"name": {"$ne": None}}},
                {"$match": {"_status": 0}}
            ]
            data_set = collections.DataSet(m.read_from(db_collect=("jingdong", last_brand_collect), out_field=("cate_id", "brand_id","name"), pipeline=pipeline))
            for i, seed in enumerate(data_set.distinct()):
                self.seeds_queue.put(Seed(value=seed, retries=kwargs["retries"], type=0))
        self.first_pettern = re.compile(r"search000014_log:{wids:'([,\d]*?)',")
        self.totalpage_perttern = re.compile(r'<div id="J_topPage"[\s\S]*?<b>\d+</b><em>/</em><i>(\d+)</i>')

    def make_request(self, seed):
        if seed.type == 0:
            cate_id, _, name = seed.value
            cid1, cid2, cid3 = re.split(',', cate_id)
            en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode({"ev": "exbrand_" + name})
            url = 'https://list.jd.com/list.html?{0}&{1}&cid3={2}'.format(en_cate_id, en_name, cid3)
            request = {"url": url,
                       "method:":"get",
                       "encoding":"utf-8",
                       "proxies": {"http": random.choice(self.proxies)},
                       "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                   "Referer": "https://list.jd.com/list.html?{0}&cid3={1}&cid2={2}".format(en_cate_id, cid3, cid2)}}
            rs = self.get_request(request)
            if rs:
                page_strs = self.totalpage_perttern.findall(rs)
                if page_strs:
                    page_strs  = page_strs[0]
                    for i in range(1, int(page_strs) + 1):
                        page,s = 2*i-1,60*(i-1)+1
                        self.seeds_queue.put(Seed(value=(seed.value[0],seed.value[1],seed.value[2],page,s), retries=self.retries, type=1))
                        self.progress_decrease()
        elif seed.type == 1:
            cate_id, _, name, page, s = seed.value
            en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode({"ev": "exbrand_" + name})
            url = 'https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1'.format(en_cate_id, en_name, page, s)
            request = {"url": url,
                       "method:":"get",
                       "encoding":"utf-8",
                       "proxies": {"http": random.choice(self.proxies)},
                       "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                   "Referer": url}}
            rs = self.get_request(request)
            if rs:
                r1 = self.first_pettern.findall(rs)
                if r1:
                    r1 = r1[0]
                    if r1:
                        buffer = []
                        for pid in r1.split(","):
                            buffer.append({"pid": pid, "cate_id": cate_id,"_seed": str(seed)})
                        if buffer:
                            self.write(buffer)
                        self.seeds_queue.put(
                            Seed(value=(seed.value[0], seed.value[1], seed.value[2], page + 1, s + 30, r1),
                                 retries=self.retries,
                                 type=2))
                        self.progress_decrease()
        elif seed.type == 2:
            cate_id, _, name, page, s, items = seed.value
            en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode({"ev": "exbrand_" + name})
            url = 'https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&scrolling=y&log_id=1596108547754.6591&tpl=1_M&isList=1&show_items={4}'.format(en_cate_id, en_name, page, s, items)
            request = {"url": url,
                       "method:":"get",
                       "encoding":"utf-8",
                       "proxies": {"http": random.choice(self.proxies)},
                       "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                   "Referer": "https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1".format(en_cate_id, en_name, page-1,s-30)}}
            rs = self.get_request(request)
            if rs:
                r1 = self.first_pettern.findall(rs)
                if r1:
                    r1 = r1[0]
                    if r1:
                        buffer = []
                        for pid in r1.split(","):
                            buffer.append({"pid":pid,"cate_id":cate_id,"_seed": str(seed)})
                        self.write(buffer)

    def parse_item(self, content, seed):
        pass


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "jdbrand"
              , "spider_num": 23
              , "retries": 3
              , "request_timeout": 10
              , "complete_timeout": 5*60
              , "sleep_interval": 0
              , "rest_time": 0
              , "write_seed": False
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jingdong",
                                 "collection": "productid" + current_date}
              , "log_config": {"level": logging.ERROR, "filename": sys.argv[0] + '.logging', "filemode":'a', "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              }
    p = GetProductId(**config)
    p.main_loop(show_process=True)
