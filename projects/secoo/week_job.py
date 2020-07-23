#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.core import HttpProxy
from multiprocess.tools import process_manger
import re
import sys
from datetime import datetime
from multiprocess.tools import timeUtil
import numpy as np


class SecooWeekJob(SpiderManger):
    def __init__(self, seeds_file, **kwargs):
        super(SecooWeekJob, self).__init__(**kwargs)
        space = np.linspace(0, 5800000, kwargs["spider_num"] + 1)
        ranges = [(int(space[i]), int(space[i + 1])) for i in range(len(space) - 1)]
        totalpages_pattern = re.compile(r'<strong>共<i>(\d+)</i>页，到第 <b>')
        self.block_pattern = re.compile(r'dlProId=[\s\W\w]*?</dl>')
        self.pid_pattern = re.compile(r'ProId="\d+"')
        self.name_pattern = re.compile(r'title=".*?"')
        self.lo_pattern = re.compile(r'"s1"[\s\W\w]*?</span>')
        self.price_pattern = re.compile(r'secoo_price.*?</span>')
        self.br_pattern = re.compile(r'</i>.*?</span')
        for r in ranges:
            page = self.get_request(request_url="http://list.secoo.com/all/0-0-0-0-0-7-0-0-0-10-{0}_{1}-0-100-0.shtml".format(r[0], r[1]))
            page_num = int(totalpages_pattern.findall(page)[0])
            self.log.info((page_num, r[0], r[1]))
            for pageindex in range(1, page_num + 1):
                self.seeds_queue.put(Seed((pageindex,r[0],r[1]), kwargs["retries"]))

    def make_requset_url(self, seed):
        return "http://list.secoo.com/all/0-0-0-0-0-7-0-0-{0}-10-{1}_{2}-0-100-0.shtml".format(*seed.value)

    def parse_item(self, content, seed):
        try:
            block = self.block_pattern.findall(content)
            for item in block:
                pid = self.pid_pattern.findall(item)
                if len(pid) > 0:
                    temp = pid[0]
                    pid = temp[7:len(temp) - 1]
                else:
                    pid = 'NA'

                name = self.name_pattern.findall(item)
                if len(name) > 0:
                    temp = name[0]
                    name = temp[7:len(temp) - 1]
                else:
                    name = 'NA'

                lo = self.lo_pattern.findall(item)
                if len(lo) > 0:
                    if len(lo) == 1:
                        temp = lo[0]
                        ziying = temp[5:len(temp) - 7]
                        lo = 'M'
                    else:
                        temp = lo[0]
                        temp1 = lo[1]
                        lo = temp[5:len(temp) - 7]
                        ziying = temp1[5:len(temp) - 7]
                else:
                    lo = 'NA'
                    ziying = 'NA'

                price = self.price_pattern.findall(item)
                if len(price) > 0:
                    temp = self.br_pattern.findall(price[0])
                    if len(temp) > 0:
                        temp = temp[0]
                        price = temp[4:len(temp) - 6]
                    else:
                        price = 'NA'
                else:
                    price = 'NA'

                result = {"code": 0, "pid": pid, "name": name, "lo": lo, "self": ziying}
        except:
            result = {"code": 1}
        return [result]


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "secoo_month_job"
              , "spider_num": 2
              , "retries": 3
              , "request_timeout": 3
              , "completetimeout": 1*60
              , "sleep_interval": 1
              , "rest_time": 10
              , "seeds_file": "resource/buyer_phone.3"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "secoo", "collection": "List" + current_date}
              , "proxies": HttpProxy.getProxy()
              , "log_config": {"level": logging.INFO, "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}
    p = SecooWeekJob(**config)
    p.main_loop(show_process=True)
