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


class Phone(SpiderManger):
    def __init__(self, seeds_file, **kwargs):
        super(Phone, self).__init__(**kwargs)
        for seed in open(seeds_file):
            self.seeds_queue.put(Seed(seed.strip("\n"), kwargs["retries"]))
        self.pro_city_pattern = re.compile(r'<dd><span>号码归属地：</span>(.*?) (.*?)</dd>')
        self.telcompany_pattern = re.compile(r'<dd><span>手机卡类型：</span>(.*?)</dd>')

    def make_requset_url(self, seed):
        return "http://shouji.xpcha.com/{0}.html".format(seed.value)

    def parse_item(self, content, seed):
        pro_city = self.pro_city_pattern.findall(content)
        tel_compay = self.telcompany_pattern.findall(content)
        result = {"code": 0, "phonenumber": seed.value, "province": pro_city[0][0],"city":(
            pro_city[0][0] if pro_city[0][1] == "" else pro_city[0][1]),"company":tel_compay[0]}
        return [result]


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "secoo_month_job"
              , "spider_num": 1
              , "retries": 3
              , "request_timeout": 3
              , "completetimeout": 1*60
              , "sleep_interval": 0.5
              , "rest_time": 0.5
              , "seeds_file": "resource/buyer_phone.3"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "secoo", "collection": "secoComment" + current_date}
              , "proxies": HttpProxy.getProxy()
              , "log_config": {"level": logging.DEBUG, "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}
    #from multiprocess.config import default_config
    #p = Phone(**default_config.config)
    p = Phone(**config)
    p.main_loop(show_process=True)
