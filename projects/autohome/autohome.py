#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.tools import process_manger
import re
import sys
import random
from multiprocess.tools import timeUtil


class JDPrice(SpiderManger):
    def __init__(self, seeds_file, **kwargs):
        super(JDPrice, self).__init__(**kwargs)
        with open(seeds_file) as infile:
            for i, seed in enumerate(infile):
                current = seed.strip('\n').split("\t")[0]
                if i % 60 == 0:
                    if i != 0:
                        self.seeds_queue.put(Seed(strr, kwargs["retries"]))
                    strr = current
                else:
                    strr = strr + '%2CJ_' + current
        if strr:
            self.seeds_queue.put(Seed(strr, kwargs["retries"]))
        self.price_ad = 'http://p.3.cn/prices/mgets?&type=1&skuIds=J_'

        self.block_pattern = re.compile(r'{.*?}')
        self.innerid_pattern = re.compile(r'\d+')
        self.innerprice_pattern = re.compile(r'"\d+.\d+"')
        self.op_pattern = re.compile(r'"op":"(\d+.\d+)"')
        self.p_pattern = re.compile(r'(\d+.\d+)"')
        self. p2_pattern = re.compile(r'(-\d+.\d+)')
        self.p1 = re.compile(r'id":.*?p":".*?"}')
        self.id_pattern = re.compile(r'id:"(\d+)"')
        self.first_pattern = re.compile(r'([a-zA-Z]*)":')
        self.rid = random.randint(100000000, 999999999)
        self.usrid = str(self.rid)
        self.up_pattern = re.compile('"up":"tpp"')


    def make_requset_url(self, seed):
        price_address = "http://p.3.cn/prices/mgets?&type=1&skuIds=J_" + seed.value + '&pduid=' + self.usrid
        return price_address

    def parse_item(self, content, seed):
        blocks = self.block_pattern.findall(content)
        result = []
        for i in blocks:
            p1s = self.p1.findall(i)
            if len(p1s) > 0:
                lines = re.split(',', p1s[0])
                if len(lines) >= 2:
                    id1 = self.p_pattern.findall(lines[0])[0]
                    info = ""
                    for j in lines:
                        up = self.up_pattern.findall(j)
                        if up != []:
                            sale = [-1]
                        else:
                            sale = self.p2_pattern.findall(j)

                            if sale == []:
                                sale = self.p_pattern.findall(j)
                        info = str(info) + '\t' + str(sale[0])
                    info = info.lstrip("\t")
                    result.append({"values": info})
        return result


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "jdprice"
              , "spider_num": 3
              , "retries": 3
              , "request_timeout": 3
              , "completetimeout": 5*60
              , "sleep_interval": 0.5
              , "rest_time": 0.5
              , "seeds_file": "/home/u9000/martingale/jd_month/month202006"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jingdong", "collection": "jdprice"+current_date}
              , "proxies": list(map(lambda x:("http://u{}:crawl@192.168.0.71:3128".format(x)), range(28)))
              , "log_config": {"level": logging.DEBUG, "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}
    p = JDPrice(**config)
    p.main_loop(show_process=True)
