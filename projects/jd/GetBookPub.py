#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.tools import process_manger
import re
import sys
import random
from multiprocess.tools import timeUtil,collections


class JDPrice(SpiderManger):
    def __init__(self, seeds_file, **kwargs):
        super(JDPrice, self).__init__(**kwargs)

        with open(seeds_file) as infile:
            data_set = collections.DataSet(infile)
            for i, seed in enumerate(data_set.map(lambda line: line.strip('\n').split("\t")[0].replace('-', ','))
                                             .shuffle(1024)):
                self.seeds_queue.put(Seed(seed, kwargs["retries"]))
        self.address = 'http://list.jd.com/list.html?cat={0}&trans=1&md={1}&my=list_{2}'
        self.pattern = re.compile(r'"id":.*?"name":".*?"')

    def make_request_url(self, seed):
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
        if result:
            return result
        else:
            return [{"seed" : seed.value}]


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "jdprice"
              , "spider_num": 23
              , "retries": 3
              , "request_timeout": 10
              , "complete_timeout": 5*60
              , "sleep_interval": 0.5
              , "rest_time": 5
              , "write_seed" : False
              , "seeds_file": "resource/newCateName"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jingdong", "collection": "brand"+current_date}
              , "proxies": list(map(lambda x:("http://u{}:crawl@192.168.0.71:3128".format(x)), range(28)))
              , "log_config": {"level": logging.INFO, "filename": sys.argv[0] + '.logging', "filemode":'a', "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}
    p = JDPrice(**config)
    p.main_loop(show_process=True)
