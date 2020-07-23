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
from mongo import op


class SecooMonthJob(SpiderManger):
    def __init__(self, **kwargs):
        super(SecooMonthJob, self).__init__(**kwargs)
        self.clean_price()

    @staticmethod
    def clean_price():
        pipeline = [
            {"$match":
                 {"pid": {"$ne": "null"}}
             }
        ]
        with op.DBManger() as m:
            dic = {}
            for collection in m.list_tables("secoo", filter={"name": {"$regex": r"List20\d\d\d\d\d\d"}}):
                for pid, price in m.read_from(db_collect=("secoo", collection), out_field=("pid", "price"), pipeline=pipeline):
                    dic.update({pid: price})
            m.drop_db_collect(db_collect=("secoo", "CleanList"))
            m.insert_many_dict(db_collect=("secoo", "CleanList"), data_dict_list=dic)

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
              , "spider_num": 23
              , "retries": 3
              , "request_timeout": 10
              , "completetimeout": 1*60
              , "sleep_interval": 10
              , "rest_time": 15
              , "seeds_file": "resource/buyer_phone.3"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "secoo", "collection": "List" + current_date}
              #, "proxies": HttpProxy.getProxy()
              , "proxies": []
              , "log_config": {"level": logging.INFO, "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close",'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'}}
    p = SecooMonthJob(**config)
    #p.main_loop(show_process=True)
