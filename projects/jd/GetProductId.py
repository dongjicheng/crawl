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
from multiprocess.core import HttpProxy


class GetProductId(SpiderManger):
    def __init__(self, **kwargs):
        super(GetProductId, self).__init__(**kwargs)
        self.retries = 3
        self.proxies = HttpProxy.getHttpProxy()
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
                self.seeds_queue.put(Seed(value=seed, retries=self.retries, type=0))
        self.first_pettern = re.compile(r"search000014_log:{wids:'([,\d]*?)',")
        self.skuids_pettern = re.compile(r'{.*?"skuId":(\d+).*?}')
        self.totalpage_perttern = re.compile(r'<div id="J_topPage"[\s\S]*?<b>\d+</b><em>/</em><i>(\d+)</i>')

    def process(self, seed):
        seed.fail()
        seed.counter.reset_count()
        buffer = []
        cate_id, _, name = seed.value
        cid1, cid2, cid3 = re.split(',', cate_id)
        if cid1 == "1713":
            en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode({"ev": "expublishers_" + name})
        else:
            en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode(
                {"ev": "exbrand_" + name})
        url = 'https://list.jd.com/list.html?{0}&{1}&cid3={2}'.format(en_cate_id, en_name, cid3)
        request = {"url": url,
                   "sleep_time": 0.1,
                   "method":"get",
                   "proxies": {"http": random.choice(self.proxies)},
                   "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                               "Referer": "https://list.jd.com/list.html?{0}&cid3={1}&cid2={2}".format(en_cate_id, cid3, cid2)}}
        rs = self.do_request(request)
        if rs:
            page_strs = self.totalpage_perttern.findall(rs)
            if page_strs:
                page_strs = page_strs[0]
                for i in range(1, int(page_strs) + 1):
                    page, s = 2*i-1, 60*(i-1)+1
                    seed1 = Seed(value=(seed.value[0],seed.value[1],seed.value[2],page,s))
                    cate_id, _, name, page, s = seed1.value
                    en_cate_id, en_name = urllib.parse.urlencode({"cat": cate_id}), urllib.parse.urlencode(
                        {"ev": "exbrand_" + name})
                    url = 'https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1'.format(en_cate_id, en_name,
                                                                                                page, s)
                    request = {"url": url,
                               "sleep_time": 0.1,
                               "method": "get",
                               "proxies": {"http": random.choice(self.proxies)},
                               "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                           "Referer": url}}
                    rs1 = self.do_request(request)
                    if rs1:
                        r1 = self.first_pettern.findall(rs1)
                        if r1:
                            r1 = r1[0]
                            if r1:
                                for pid in r1.split(","):
                                    seed.counter.increase()
                                    request = {"url": "https://item.jd.com/{}.html".format(pid),
                                           "sleep_time": 0.1,
                                           "method": "get",
                                           "proxies": {"http": random.choice(self.proxies)},
                                           "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                                       "Referer": "https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1".format(
                                                           en_cate_id, en_name, page, s)}}
                                    rs2 = self.do_request(request)
                                    if rs2:
                                        r2 = self.skuids_pettern.findall(rs2)
                                        for skuid in r2:
                                            buffer.append({"skuid": skuid, "cate_id": cate_id, "_seed": str(seed)})

                                seed2 = Seed(value=(seed.value[0], seed.value[1], seed.value[2], page + 1, s + 30, r1))
                                cate_id, _, name, page, s, items = seed2.value
                                en_cate_id, en_name = urllib.parse.urlencode(
                                    {"cat": cate_id}), urllib.parse.urlencode({"ev": "exbrand_" + name})
                                url = 'https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&scrolling=y&log_id=1596108547754.6591&tpl=1_M&isList=1&show_items={4}'.format(
                                    en_cate_id, en_name, page, s, items)
                                request = {"url": url,
                                           "sleep_time": 0.1,
                                           "method": "get",
                                           "proxies": {"http": random.choice(self.proxies)},
                                           "headers": {"Connection": "close", "User-Agent": self.ua.chrome,
                                                       "Referer": "https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1".format(
                                                           en_cate_id, en_name, page - 1, s - 30)}}
                                rs3 = self.do_request(request)
                                if rs3:
                                    r3 = self.first_pettern.findall(rs3)
                                    if r3:
                                        r3 = r3[0]
                                        if r3:
                                            for pid in r3.split(","):
                                                seed.counter.increase()
                                                request = {"url": "https://item.jd.com/{}.html".format(pid),
                                                           "sleep_time": 0.1,
                                                           "method": "get",
                                                           "proxies": {"http": random.choice(self.proxies)},
                                                           "headers": {"Connection": "close",
                                                                       "User-Agent": self.ua.chrome,
                                                                       "Referer": "https://list.jd.com/list.html?{0}&{1}&page={2}&s={3}&click=1".format(
                                                                           en_cate_id, en_name, page-1, s-30)}}
                                                rs4 = self.do_request(request)
                                                if rs4:
                                                    r4 = self.skuids_pettern.findall(rs4)
                                                    for skuid in r4:
                                                        buffer.append(
                                                            {"skuid": skuid, "cate_id": cate_id, "_seed": str(seed)})
                if seed.counter.get_count() > 60*(int(page_strs)-1):
                    self.write(buffer)
                    seed.ok()


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "jdproductid"
              , "spider_num": 23
              , "complete_timeout": 5*60
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jingdong",
                                 "collection": "productid" + current_date}
              , "log_config": {"level": logging.ERROR, "filename": sys.argv[0] + '.logging', "filemode":'a', "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              }
    p = GetProductId(**config)
    p.main_loop(show_process=True)
