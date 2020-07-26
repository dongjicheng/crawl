#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.core import HttpProxy
from multiprocess.tools import process_manger
from multiprocess.tools import stringUtils
import re
import sys


class Phone(SpiderManger):
    def __init__(self, seeds_file, **kwargs):
        super(Phone, self).__init__(**kwargs)
        self.phone_regx = re.compile(r'^\d{11,11}$')
        self.phone_number_checker = stringUtils.check_legality(pattern=r'^\d{11,11}$')
        for seed in open(seeds_file):
            seed = seed.strip("\n")
            if(self.phone_number_checker(seed)):
                self.seeds_queue.put(Seed(seed, kwargs["retries"]))
            else:
                self.log.info("legal_format: " + seed)
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
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "shoujiguishudi"
              , "spider_num": 23
              , "retries": 3
              , "request_timeout": 10
              , "complete_timeout": 1*60
              , "sleep_interval": 0.5
              , "rest_time": 0.5
              , "write_seed" : True
              , "seeds_file": "resource/buyer_phone.3"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jicheng", "collection": "shoujiguishudi"}
              , "proxies": HttpProxy.getProxy()
              , "log_config": {"level": logging.INFO, "filename": sys.argv[0] + '.logging', "filemode":'a', "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}
    #from multiprocess.config import default_config
    #p = Phone(**default_config.config)
    p = Phone(**config)
    p.main_loop(show_process=True)
